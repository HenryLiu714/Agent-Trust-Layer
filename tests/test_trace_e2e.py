"""Trace format v1 end to end: what `irimi shadow` records, encoded the way the trace store will.

The unit tests in `tests/test_trace.py` prove the codecs round-trip an Exchange built by hand, and
the engine tests prove a real engine stamps its exchanges. Neither proves the claim #68 is for: a
run of the real CLI, over the Phase 2 scenario, hands its store exchanges that survive the trip to
an `events.jsonl` line and back with every Phase 2 field intact and every span in order.

`cli._build_engine` imports `NullStore` when it runs, so each test here swaps in `_EventsStore`,
which does to every exchange exactly what P3-04 (#70) will do: `trace.event_to_json` with a
content-addressed blob dict, `json.dumps(..., allow_nan=False)`, and back through `json.loads` and
`trace.event_from_json`. Nothing else in the composition changes.
"""

import dataclasses
import json
import sys
import time

import pytest

from irimi import redact, runner, trace
from irimi.cli import main
from irimi.exchange import Exchange
from tests import test_engine_mitm, test_phase_exit
from tests.test_engine_mitm import (
    _carries_run_header,
    _Upstream,
)
from tests.test_phase_exit import (
    CHARGE_ID,
    CHILD,
    PHASE2_AMOUNT,
    PHASE2_CHARGE,
    REFUND_AMOUNT_MINOR,
    _run_phase2_under_shadow,
    _StandInReader,
    _Stripe,
)

# Their fixtures, bound here so pytest finds them: an import would be shadowed by each parameter.
home = test_phase_exit.home
stripe_stand_in = test_phase_exit.stripe_stand_in
upstream = test_engine_mitm.upstream

# The fields the roadmap names: "the exchange record has to carry every field Phase 2 added, or
# the Phase 4 report cannot print what the live summary prints" (Notion, Phase 3; #68).
PHASE2_FIELDS = ("precondition", "rejection_code", "overlay", "issued_by", "would_fire", "currency")


class _EventsStore:
    """A `TraceStore` that writes what #70 will write, in memory. `seq` is assigned as each finished
    exchange arrives, which is the ordering rule: completion order (#68). An encoding failure is
    kept rather than raised, because a store that raises inside a hook is a different bug."""

    built: list["_EventsStore"] = []

    def __init__(self) -> None:
        self.received: list[Exchange] = []
        self.lines: list[str] = []
        self.blobs: dict[str, bytes] = {}
        self.errors: list[Exception] = []
        self.closed = False
        _EventsStore.built.append(self)

    def record(self, exchange: Exchange) -> None:
        self.received.append(exchange)
        seq = len(self.received)  # from 1, as #70's writer counts
        try:
            line = trace.event_to_json(seq, exchange, self._put_body)
            self.lines.append(json.dumps(line, allow_nan=False))
        except Exception as exc:
            self.errors.append(exc)

    def close(self) -> None:
        self.closed = True

    def _put_body(self, body: bytes) -> trace.BodyRef:
        ref = trace.body_ref(body)
        self.blobs[ref.sha256] = body
        return ref

    def _get_body(self, ref: trace.BodyRef) -> bytes:
        body = self.blobs[ref.sha256]
        assert (len(body), ref.truncated) == (ref.size, False)
        return body

    def events(self) -> list[tuple[int, trace.Event]]:
        return [trace.event_from_json(json.loads(line), self._get_body) for line in self.lines]


@pytest.fixture
def events_store(monkeypatch):
    """Returns a function giving the one store the run built, once it has been closed."""
    _EventsStore.built = []
    monkeypatch.setattr("irimi.store.NullStore", _EventsStore)

    def the_store() -> _EventsStore:
        (store,) = _EventsStore.built
        assert store.closed
        assert store.errors == []
        return store

    return the_store


def _decoded(store: _EventsStore) -> list[Exchange]:
    """Every line back, in `seq` order, each equal to the exchange the engine handed the store."""
    events = store.events()
    assert [seq for seq, _ in events] == list(range(1, len(store.received) + 1))
    decoded = [event for _, event in events if isinstance(event, Exchange)]
    assert decoded == store.received
    return decoded


def test_the_phase_2_run_under_irimi_shadow_survives_the_trace_format_event_for_event(
    home, tmp_path, monkeypatch, events_store
):
    """#68's round trip through the real composition. Every exchange `irimi shadow` hands its
    store over the Phase 2 scenario - the agent's five calls and the engine's two L3 reads -
    encodes to an `events.jsonl` line and decodes back equal, and the six fields Phase 2 added each
    arrive with the value the live summary prints from."""
    _run_phase2_under_shadow(tmp_path, monkeypatch)

    store = events_store()
    decoded = _decoded(store)
    shape = [(ex.issued_by, ex.request.method, ex.request.path, ex.answered_by) for ex in decoded]
    assert shape == [
        ("agent", "GET", "/v1/charges", "live"),
        ("engine", "GET", f"/v1/charges/{PHASE2_CHARGE}", "live"),
        ("agent", "POST", "/v1/refunds", "fake-L1"),
        ("agent", "GET", "/v1/refunds", "overlay"),
        ("agent", "GET", f"/v1/charges/{PHASE2_CHARGE}", "overlay"),
        ("engine", "GET", f"/v1/charges/{PHASE2_CHARGE}", "live"),
        ("agent", "POST", "/v1/refunds", "fake-L1"),
    ]
    _, check, refund, refunds, charge, recheck, retry = decoded
    assert (refund.precondition, refund.rejection_code, refund.currency) == ("passed", "", "usd")
    assert refund.would_fire == ("refund.created", "charge.refunded")
    assert (retry.precondition, retry.rejection_code, retry.currency) == (
        "rejected",
        "charge_already_refunded",
        "usd",
    )
    assert retry.would_fire == ()
    assert (refunds.overlay, charge.overlay) == ("full", "full")
    assert (check.issued_by, recheck.issued_by) == ("engine", "engine")
    defaults = {f.name: f.default for f in dataclasses.fields(Exchange)}
    for name in PHASE2_FIELDS:
        assert any(getattr(ex, name) != defaults[name] for ex in decoded), name

    # Bodies never go inline: the refund's form body is a ref in the line and a blob beside it,
    # and the retry posted the same bytes, so both lines name one blob.
    posted = f"charge={PHASE2_CHARGE}&amount={PHASE2_AMOUNT}".encode()
    assert not any(posted.decode() in line for line in store.lines)
    assert store.blobs[trace.body_ref(posted).sha256] == posted
    refund_line, retry_line = (json.loads(store.lines[i]) for i in (2, 6))
    assert refund_line["request"]["body"] == retry_line["request"]["body"]


def test_every_exchange_the_phase_2_run_stores_is_timed_and_seq_is_completion_order(
    home, tmp_path, monkeypatch, events_store
):
    """#68's timestamps, read back off the decoded lines. Every exchange spans `0 < started_at <=
    ended_at` inside the run; each engine-issued L3 read spans its `Reader` call, inside the write
    it checked; and `seq` is completion order, so the read is stored before its write while the
    write started first - `started_at` is what recovers start order."""
    before = time.time()
    _run_phase2_under_shadow(tmp_path, monkeypatch)
    after = time.time()
    decoded = _decoded(events_store())

    for ex in decoded:
        assert 0 < before <= ex.started_at <= ex.ended_at <= after, ex.request.path
    read, check, refund, refunds, charge, recheck, retry = decoded
    for issued, write in ((check, refund), (recheck, retry)):
        assert write.started_at < issued.started_at <= issued.ended_at <= write.ended_at
    ended = [ex.ended_at for ex in decoded]
    assert ended == sorted(ended)
    by_start = sorted(decoded, key=lambda ex: ex.started_at)
    assert by_start == [read, refund, check, refunds, charge, retry, recheck]


# Each call names itself in its query, so the recorded exchange can be matched to what it sent.
# Placeholders rather than `str.format`, as in `tests.test_phase_exit`'s children. `putheader`
# rather than a dict, because a dict cannot send one header twice.
RUN_HEADER_CHILD = """
import http.client, os, urllib.parse

proxy = urllib.parse.urlparse(os.environ["HTTP_PROXY"])
authority = "127.0.0.1:__PORT__"
print("RUN", os.environ["__RUN_ENV__"])


def call(method, case, sent):
    conn = http.client.HTTPConnection(proxy.hostname, proxy.port, timeout=10)
    conn.putrequest(method, "http://" + authority + "/hello?case=" + case, skip_host=True)
    conn.putheader("host", authority)
    for name, value in sent:
        conn.putheader(name, value)
    body = b"{}" if method == "POST" else None
    if body is not None:
        conn.putheader("content-type", "application/json")
        conn.putheader("content-length", str(len(body)))
    conn.endheaders(body)
    resp = conn.getresponse()
    resp.read()
    print("CALL", case, resp.status)
    conn.close()


call("GET", "dotdot", [("Irimi-Run", "../x")])
call("GET", "valid", [("Irimi-Run", "run_A-1")])
call("GET", "repeated", [("Irimi-Run", "../x"), ("Irimi-Run", "run_B")])
call("GET", "upper", [("IRIMI-RUN", "run_C")])
call("GET", "padded", [("irimi-run", "  run_D  ")])
call("GET", "long", [("Irimi-Run", "a" * 65)])
call("GET", "space", [("Irimi-Run", "a b")])
call("POST", "write", [("Irimi-Run", "../x")])
"""


def test_an_invalid_run_header_from_a_shadowed_child_names_no_run_and_reaches_no_service(
    home, tmp_path, capfd, upstream, events_store
):
    """#68's path-traversal guard through the real CLI, with #67's strip beside it. A run id
    becomes a directory in the trace store, so a value `trace.is_valid_run_id` refuses - `../x`,
    65 characters, a space - is no run id and the exchange keeps the engine's own, read or faked
    write alike. A valid one names its run in any spelling of the header name, trimmed, and a
    repeated header's first valid value wins. Whatever the value, no service ever sees the header
    and no stored request carries it."""
    child = tmp_path / "child.py"
    child.write_text(
        RUN_HEADER_CHILD.replace("__PORT__", str(upstream)).replace("__RUN_ENV__", runner.RUN_ENV)
    )
    assert main(["shadow", "--port", "0", "--", sys.executable, str(child)]) == 0
    out = capfd.readouterr().out.splitlines()
    (engine_run,) = [line.split()[1] for line in out if line.startswith("RUN ")]
    assert f"irimi shadow · run {engine_run} · listening on" in "\n".join(out)
    assert trace.is_valid_run_id(engine_run)
    assert len([line for line in out if line.startswith("CALL ") and line.endswith(" 200")]) == 8

    assert len(_Upstream.seen) == 7  # the seven GETs; the POST was faked and never left
    for path, received in _Upstream.seen:
        assert not _carries_run_header(received), f"{path} reached the service carrying Irimi-Run"
    decoded = _decoded(events_store())
    by_case = {ex.request.query.removeprefix("case="): ex for ex in decoded}
    assert {case: ex.run_id for case, ex in by_case.items()} == {
        "dotdot": engine_run,
        "valid": "run_A-1",
        "repeated": "run_B",
        "upper": "run_C",
        "padded": "run_D",
        "long": engine_run,
        "space": engine_run,
        "write": engine_run,
    }
    assert by_case["write"].answered_by == "fake-L0"
    for ex in decoded:
        assert not _carries_run_header(ex.request.headers)
        assert 0 < ex.started_at <= ex.ended_at


def test_the_phase_1_run_through_the_reverse_door_survives_the_trace_format(
    home, tmp_path, monkeypatch, stripe_stand_in, events_store
):
    """The Phase 1 criterion's run (#13), stored: a read through the forward proxy, and a refund
    through the reverse door answered from the SHIPPED Stripe map's fixture after an L3 read of
    the charge. Every exchange decodes equal and is timed, the refund's `door: reverse` included,
    and the engine's read sits inside the refund's span."""
    _run_phase1_under_shadow(tmp_path, monkeypatch, stripe_stand_in)

    read, check, refund = _decoded(events_store())
    assert [(ex.door, ex.issued_by, ex.kind) for ex in (read, check, refund)] == [
        ("forward", "agent", "read"),
        ("forward", "engine", "read"),
        ("reverse", "agent", "write"),
    ]
    assert (refund.request.host, refund.answered_by, refund.currency) == (
        "api.stripe.com",
        "fake-L1",
        "usd",
    )
    assert refund.precondition == "passed"
    for ex in (read, check, refund):
        assert 0 < ex.started_at <= ex.ended_at
    assert refund.started_at < check.started_at <= check.ended_at <= refund.ended_at


def _run_phase1_under_shadow(tmp_path, monkeypatch, stripe_port: int) -> None:
    """The Phase 1 criterion's child (`tests.test_phase_exit.CHILD`) under the real `irimi shadow`,
    against the loopback Stripe, which never hears a write. Its calls carry
    `authorization: Bearer sk_test_notreal`."""
    monkeypatch.setattr("irimi.reader.UpstreamReader", _StandInReader)
    _StandInReader.port = stripe_port
    child = tmp_path / "child.py"
    child.write_text(
        CHILD.replace("__CHARGES_PORT__", str(stripe_port))
        .replace("__CHARGE__", CHARGE_ID)
        .replace("__AMOUNT__", str(REFUND_AMOUNT_MINOR))
    )
    assert main(["shadow", "--port", "0", "--", sys.executable, str(child)]) == 0
    assert [m for m, _, _ in _Stripe.seen if m != "GET"] == []


def test_the_phase_1_run_redacted_for_disk_carries_no_credential_and_changes_nothing_else(
    home, tmp_path, monkeypatch, stripe_stand_in, events_store
):
    """#69 over what a real `irimi shadow` run hands its store. Each exchange, redacted under this
    install's key the way #70's writer will and then encoded, holds no `sk_test_` anywhere - lines
    or blobs - and one placeholder for the one bearer token wherever it was sent, the engine's own
    L3 read included. Nothing else changes: put each request's headers back and the redacted copy
    equals what the engine recorded, bodies byte for byte."""
    _run_phase1_under_shadow(tmp_path, monkeypatch, stripe_stand_in)
    received = events_store().received
    key = redact.load_key(home)
    stored = [redact.redact_exchange(ex, key) for ex in received]

    blobs: dict[str, bytes] = {}

    def put_body(body: bytes) -> trace.BodyRef:
        ref = trace.body_ref(body)
        blobs[ref.sha256] = body
        return ref

    lines = [json.dumps(trace.exchange_to_json(ex, put_body)) for ex in stored]
    assert all("sk_test_" not in line for line in lines)
    assert all(b"sk_test_" not in blob for blob in blobs.values())
    bearer = redact.placeholder(key, "Bearer sk_test_notreal")
    sent = [ex.request.header("authorization") for ex in stored]
    assert set(sent) == {bearer}, sent
    for live, disk in zip(received, stored, strict=True):
        restored = dataclasses.replace(
            disk, request=dataclasses.replace(disk.request, headers=live.request.headers)
        )
        assert restored == live
    assert received[0].request.header("authorization") == "Bearer sk_test_notreal"
