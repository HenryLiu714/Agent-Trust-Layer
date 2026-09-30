"""Trace format v1 end to end: what `irimi shadow` stores, read back off disk (#68, #69, #70).

The unit tests in `tests/test_trace.py` and `tests/test_store.py` prove the codecs and the store
over exchanges built by hand, and the engine tests prove a real engine stamps its exchanges. None
of them proves the claim #70 is for: a run of the real CLI, over the Phase 2 scenario, leaves a run
on disk whose exchanges read back as exactly what the engine handed its store - redacted, with
every Phase 2 field intact and every span in order.

`cli._open_store` imports `DirectoryStore` when it runs, so each test here swaps in `_SpyStore`:
the real store, unchanged, which also remembers each exchange the engine handed it. Every
assertion is made on what `StoreReader` reads back and on the files under the store's root.

The last part holds #69's redaction to the same standard: real traffic through the real
`irimi shadow`, both doors, against loopback stand-ins, with every assertion made on the stored
lines and blobs.
"""

import base64
import dataclasses
import json
import shutil
import socket
import sys
import threading
import time
from bisect import bisect_left
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from itertools import accumulate, pairwise
from pathlib import Path

import pytest

from irimi import redact, report, runner, trace
from irimi.bodies import FORM_CT
from irimi.cli import main
from irimi.exchange import (
    FIDELITY_L0_FLAG,
    TARGET_FAILED_FLAG,
    UNCLASSIFIED_FLAG,
    Exchange,
    Headers,
    header_value,
    media_type,
)
from irimi.servicemap import loader
from irimi.store import DirectoryStore, StoredRun, StoreReader
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


class _SpyStore(DirectoryStore):
    """The store `irimi shadow` opens, unchanged, remembering each exchange the engine handed it,
    in the order it did: the live exchanges a stored one is compared against."""

    built: list["_SpyStore"] = []

    def __init__(self, root: Path, key: bytes) -> None:
        super().__init__(root, key)
        self.received: list[Exchange] = []
        _SpyStore.built.append(self)

    def record(self, exchange: Exchange) -> None:
        self.received.append(exchange)
        super().record(exchange)


@dataclasses.dataclass(frozen=True)
class _Disk:
    """A store's root, read back once the run that wrote it has returned."""

    root: Path
    key: bytes
    received: list[Exchange]  # what the engine handed the store, in order

    @property
    def reader(self) -> StoreReader:
        return StoreReader(self.root)

    def process_run(self) -> StoredRun:
        """The one run `irimi shadow` started for its child."""
        (record,) = [r for r in self.reader.list_runs() if r.attribution == "process"]
        return self.reader.load_run(record.run_id)

    def exchanges(self) -> list[Exchange]:
        """Every stored exchange: run by run, each in `seq` order, then the unattributed ones."""
        ids = [r.run_id for r in self.reader.list_runs()] + [trace.UNATTRIBUTED]
        runs = [self.reader.load_run(run_id) for run_id in ids]
        return [event for run in runs for event in run.events if isinstance(event, Exchange)]

    def lines(self, run_id: str) -> list[str]:
        return (self.root / "runs" / run_id / "events.jsonl").read_text().splitlines()

    def blobs(self) -> dict[str, bytes]:
        blobs = self.root / "blobs"
        return {path.name: path.read_bytes() for path in blobs.iterdir()} if blobs.is_dir() else {}

    def files_holding(self, needle: bytes) -> list[Path]:
        """Every file under the root whose bytes hold `needle`: lines, blobs and run records."""
        return [p for p in self.root.rglob("*") if p.is_file() and needle in p.read_bytes()]


@pytest.fixture
def stored(home, monkeypatch):
    """Returns a function giving the store the run opened, read back after `main` returned. The
    store dropped nothing and has nothing left to write, and it was handed exactly the exchanges
    `irimi shadow`'s `on_exchange` printed, in the same order: the live exchanges #70's
    done-when compares a stored run against."""
    _SpyStore.built = []
    monkeypatch.setattr("irimi.store.DirectoryStore", _SpyStore)
    printed: list[Exchange] = []
    line = report.exchange_line

    def printing(ex: Exchange) -> str:
        printed.append(ex)
        return line(ex)

    monkeypatch.setattr("irimi.report.exchange_line", printing)

    def the_disk() -> _Disk:
        (store,) = _SpyStore.built
        assert (store.stats().queued, store.stats().dropped) == (0, 0)
        assert len(store.received) == len(printed)
        assert all(a is b for a, b in zip(store.received, printed, strict=True))
        return _Disk(store.layout.root, redact.load_key(home), store.received)

    return the_disk


def _assert_same(stored: Exchange, expected: Exchange) -> None:
    """Field by field over `dataclasses.fields(Exchange)`, so a failure names the field."""
    for field in dataclasses.fields(Exchange):
        assert getattr(stored, field.name) == getattr(expected, field.name), field.name


def _decoded(disk: _Disk) -> list[Exchange]:
    """The process run's exchanges read back off disk, in `seq` order: each is the exchange the
    engine handed the store, redacted (#69), and `seq` runs 1..n with no gap."""
    run = disk.process_run()
    seqs = [json.loads(line)["seq"] for line in disk.lines(run.record.run_id)]
    assert seqs == list(range(1, len(disk.received) + 1))
    decoded = [event for event in run.events if isinstance(event, Exchange)]
    for stored, live in zip(decoded, disk.received, strict=True):
        _assert_same(stored, redact.redact_exchange(live, disk.key))
    return decoded


def test_the_phase_2_run_under_irimi_shadow_survives_the_trace_format_event_for_event(
    home, tmp_path, monkeypatch, stored
):
    """#70's end-to-end test, and #68's round trip through the real composition. The Phase 2
    scenario run with `--store` leaves one run directory: the process run, with its argv and
    `outcome: ok`. Every exchange `irimi shadow` handed its store - the agent's five calls and the
    engine's two L3 reads - reads back off disk equal to itself redacted, and the six fields Phase
    2 added each arrive with the value the live summary prints from."""
    root = tmp_path / "trace"
    _run_phase2_under_shadow(tmp_path, monkeypatch, "--store", str(root))

    disk = stored()
    assert disk.root == root
    run = disk.process_run()
    assert [path.name for path in (root / "runs").iterdir()] == [run.record.run_id]
    assert run.record.attribution == "process"
    assert run.record.trigger is not None
    assert run.record.trigger.args == {"argv": [sys.executable, str(tmp_path / "child.py")]}
    assert (run.record.outcome, run.record.exit_code, run.record.error) == ("ok", 0, None)
    assert run.record.dropped_events == 0
    decoded = _decoded(disk)
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
    lines = disk.lines(run.record.run_id)
    assert not any(posted.decode() in line for line in lines)
    assert disk.blobs()[trace.body_ref(posted).sha256] == posted
    refund_line, retry_line = (json.loads(lines[i]) for i in (2, 6))
    assert refund_line["request"]["body"] == retry_line["request"]["body"]


def test_every_exchange_the_phase_2_run_stores_is_timed_and_seq_is_completion_order(
    home, tmp_path, monkeypatch, stored
):
    """#68's timestamps, read back off disk. Every exchange spans `0 < started_at <= ended_at`
    inside the run; each engine-issued L3 read spans its `Reader` call, inside the write it
    checked; and `seq` is completion order, so the read is stored before its write while the write
    started first - `started_at` is what recovers start order. The run itself spans them all."""
    before = time.time()
    _run_phase2_under_shadow(tmp_path, monkeypatch)
    after = time.time()
    disk = stored()
    decoded = _decoded(disk)

    for ex in decoded:
        assert 0 < before <= ex.started_at <= ex.ended_at <= after, ex.request.path
    read, check, refund, refunds, charge, recheck, retry = decoded
    for issued, write in ((check, refund), (recheck, retry)):
        assert write.started_at < issued.started_at <= issued.ended_at <= write.ended_at
    ended = [ex.ended_at for ex in decoded]
    assert ended == sorted(ended)
    by_start = sorted(decoded, key=lambda ex: ex.started_at)
    assert by_start == [read, refund, check, refunds, charge, retry, recheck]
    record = disk.process_run().record
    assert record.started_at is not None and record.ended_at is not None
    assert before <= record.started_at <= read.started_at
    assert retry.ended_at <= record.ended_at <= after


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
    home, tmp_path, capfd, upstream, stored
):
    """#68's path-traversal guard through the real CLI, with #67's strip beside it. A run id
    becomes a directory in the trace store, so a value `trace.is_valid_run_id` refuses - `../x`,
    65 characters, a space - is no run id and the exchange keeps the engine's own, read or faked
    write alike. A valid one names its run in any spelling of the header name, trimmed, and a
    repeated header's first valid value wins. Whatever the value, no service ever sees the header
    and no stored request carries it. On disk, each valid id is a `header` run of its own (#70),
    and nothing is created outside the store's root."""
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
    disk = stored()
    decoded = disk.exchanges()
    assert sorted(decoded, key=lambda ex: ex.request.query) == sorted(
        (redact.redact_exchange(ex, disk.key) for ex in disk.received),
        key=lambda ex: ex.request.query,
    )
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
    runs = {r.run_id: r.attribution for r in disk.reader.list_runs()}
    assert runs == {
        engine_run: "process",
        "run_A-1": "header",
        "run_B": "header",
        "run_C": "header",
        "run_D": "header",
    }
    assert {p.name for p in disk.root.iterdir()} <= {"blobs", "runs", "unattributed"}
    assert not (disk.root / "x").exists() and not (disk.root.parent / "x").exists()


def test_the_phase_1_run_through_the_reverse_door_survives_the_trace_format(
    home, tmp_path, monkeypatch, stripe_stand_in, stored
):
    """The Phase 1 criterion's run (#13), stored: a read through the forward proxy, and a refund
    through the reverse door answered from the SHIPPED Stripe map's fixture after an L3 read of
    the charge. Every exchange decodes equal and is timed, the refund's `door: reverse` included,
    and the engine's read sits inside the refund's span."""
    _run_phase1_under_shadow(tmp_path, monkeypatch, stripe_stand_in)

    read, check, refund = _decoded(stored())
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
    home, tmp_path, monkeypatch, stripe_stand_in, stored
):
    """#69 over what a real `irimi shadow` run stored (#70). No file under the store's root holds
    `sk_test_` - no line, no blob, no run record - and the one bearer token is one placeholder
    wherever it was sent, the engine's own L3 read included. Nothing else changes: swap the token
    for its placeholder in what the engine recorded and it equals what was stored, bodies byte
    for byte."""
    _run_phase1_under_shadow(tmp_path, monkeypatch, stripe_stand_in)
    disk = stored()
    bearer = "Bearer sk_test_notreal"
    written = _assert_stored_as(disk, _placeholders(disk.key, bearer))
    assert disk.files_holding(b"sk_test_") == []
    sent = [ex.request.header("authorization") for ex in written]
    assert set(sent) == {redact.placeholder(disk.key, bearer)}, sent
    assert disk.received[0].request.header("authorization") == bearer


# ------------------------------------------------------------ redaction over real traffic (#69)
#
# Each rule of #69 that real traffic exercises, through the real `irimi shadow`: the child below
# makes the calls a test hands it, through the forward proxy or the reverse door, and prints what
# it was answered. The store's writer redacts each exchange before it writes it (#70), and
# `_assert_stored_as` holds the two promises over the files it wrote: no secret is in any of
# them, and swapping each secret for its placeholder in what the engine recorded gives exactly
# what was stored - so one secret has one placeholder everywhere it went, and every other byte
# survives. Every upstream is loopback: the stand-in below, or nothing at all.


def _placeholders(key: bytes, *secrets: str) -> dict[str, str]:
    """Each secret and the placeholder it must be stored as."""
    return {secret: redact.placeholder(key, secret) for secret in secrets}


def _with_placeholders(ex: Exchange, hidden: dict[str, str]) -> Exchange:
    """`ex` with each secret in `hidden` swapped for its stand-in in every part `redact` reads: the
    request's path, query, headers and body, the response's headers and body, the answer target
    and the operation. Longest secret first, so a header value holding a shorter secret is swapped
    whole.

    A body is swapped byte for byte, then held to #69's rule 5: a JSON body a rule changed is
    re-serialized compact, and every other body - a form, a stream, bytes that are not UTF-8 -
    keeps its bytes. A streamed body's chunks then follow it (`_chunks_after`, #71)."""
    order = sorted(hidden, key=len, reverse=True)

    def text(value: str) -> str:
        for secret in order:
            value = value.replace(secret, hidden[secret])
        return value

    def body(value: bytes, pairs: Headers) -> bytes:
        swapped = value
        for secret in order:
            swapped = swapped.replace(secret.encode(), hidden[secret].encode())
        if swapped == value or media_type(header_value(pairs, "content-type") or "") == FORM_CT:
            return swapped
        try:
            document = json.loads(swapped)
        except ValueError:
            return swapped
        return json.dumps(document, separators=(",", ":"), ensure_ascii=False).encode()

    def headers(pairs: Headers) -> Headers:
        return tuple((name, text(value)) for name, value in pairs)

    request = dataclasses.replace(
        ex.request,
        path=text(ex.request.path),
        query=text(ex.request.query),
        headers=headers(ex.request.headers),
        body=body(ex.request.body, ex.request.headers),
    )
    response = ex.response
    if response is not None:
        response = dataclasses.replace(
            response,
            headers=headers(response.headers),
            body=body(response.body, response.headers),
        )
    stream_chunks = ex.stream_chunks
    if response is not None and ex.response is not None and stream_chunks:
        stream_chunks = _chunks_after(ex.response.body, response.body, stream_chunks)
    return dataclasses.replace(
        ex,
        request=request,
        response=response,
        target=text(ex.target),
        operation=text(ex.operation),
        stream_chunks=stream_chunks,
    )


def _chunks_after(sent: bytes, stored: bytes, chunks: tuple[int, ...]) -> tuple[int, ...]:
    """#71's rule for a stream's chunks once its body is redacted, stated apart from `redact`'s
    own: the two bodies pair up line for line; a boundary in an unchanged line keeps its place in
    it, one in a changed line moves to that line's end, and boundaries that meet make one chunk."""
    if sent == stored:
        return chunks
    old, new = sent.splitlines(keepends=True), stored.splitlines(keepends=True)
    assert len(old) == len(new), "a stream the corpus redacts keeps its lines"
    old_ends, new_ends = list(accumulate(map(len, old))), list(accumulate(map(len, new)))
    cuts = set()
    for boundary in accumulate(chunks[:-1]):
        i = bisect_left(old_ends, boundary)
        if old[i] == new[i]:
            cuts.add(new_ends[i] - len(new[i]) + boundary - (old_ends[i] - len(old[i])))
        else:
            cuts.add(new_ends[i])
    edges = [0, *sorted(cut for cut in cuts if 0 < cut < len(stored)), len(stored)]
    return tuple(b - a for a, b in pairwise(edges))


def _assert_stored_as(disk: _Disk, hidden: dict[str, str]) -> list[Exchange]:
    """#69's two promises over what the store wrote (#70): no secret in `hidden` is in any file
    under its root, and each stored exchange is the one the engine handed the store with exactly
    those secrets swapped for their placeholders - every field compared, bodies byte for byte.
    Returns the stored exchanges."""
    for secret in hidden:
        assert disk.files_holding(secret.encode()) == [], secret
    written = disk.exchanges()
    for live, stored in zip(disk.received, written, strict=True):
        _assert_same(stored, _with_placeholders(live, hidden))
    return written


# The stand-in's own secrets and answers. Each is a value no rule would touch unless it is one.
WEBHOOK_TOKEN = "xq9WebhookPathSecret24"  # a real one is 24 alphanumerics, no credential shape
WEBHOOK_PATH = f"/services/T0REDACT/B0REDACT/{WEBHOOK_TOKEN}"
WEBHOOK_BODY = b'{"text":"deploy finished"}'
QUERY_API_KEY = "qs-secret-api-key-4f2"  # a secret by its name only, like CARD_TOKEN
CARD_TOKEN = "tok_redact_e2e"
STRIPE_BEARER = "Bearer sk_test_RedactE2e"
STRIPE_FORM = f"card[token]={CARD_TOKEN}&amount=100".encode()
LLM_API_KEY = "sk-ant-api03-RedactE2eHeaderKey"
NESTED_KEY = "sk_live_NestedThreeDeep"
LLM_TOKEN = "plain-session-token"  # under the whole key `token`, beside `max_tokens`
LLM_BODY = json.dumps(
    {
        "model": "standin-1",
        "max_tokens": 1024,
        "metadata": {"agent": {"env": {"key": NESTED_KEY}}},
        "token": LLM_TOKEN,
        "messages": [{"role": "user", "content": "hi"}],
    }
).encode()
# The model echoes the key back, so one secret is stored in both a request and a response.
LLM_ANSWER = json.dumps(
    {
        "id": "msg_1",
        "content": [{"type": "text", "text": f"your key is {NESTED_KEY}"}],
        "usage": {"input_tokens": 3, "output_tokens": 5},
    }
).encode()
SSE_PLAIN_KEY = "sk_live_StreamedPlain"
SSE_ESCAPED_KEY = "sk_live_AfterAnEscape"
# The second event's key follows a JSON `\n` escape: a backslash and an `n`, as the wire has it.
SSE_BODY = (
    b'data: {"text":"key: ' + SSE_PLAIN_KEY.encode() + b'"}\n\n'
    b'data: {"text":"key:\\n' + SSE_ESCAPED_KEY.encode() + b'"}\n\n'
)
SESSION_COOKIE = "session=cookie-secret-77; Path=/; HttpOnly"
BINARY_BODY = b"\x89PNG\r\n\x1a\n\xff\xfe\x00 not UTF-8, stored as it came"

# The stand-in's host, claimed as a service so its routes classify: an LLM that answers JSON or a
# stream, and a read that sets a cookie over a binary body. Added beside the shipped maps, which
# the Slack webhook and the Stripe write need.
STANDIN_MAP = """
version: 1
service: standin
hosts:
  - 127.0.0.1
routes:
  - match:
      method: POST
      path: /v1/messages
    operation: messages.create
    kind: llm
    human: ask the model
  - match:
      method: POST
      path: /v1/stream
    operation: messages.stream
    kind: llm
    human: stream the model
  - match:
      method: GET
      path: /v1/session
    operation: session.get
    kind: read
    human: read the session
"""


class _RedactStandIn(BaseHTTPRequestHandler):
    """Everything on the other side of the proxy in the redaction runs, and the webhook's answer
    target. It records every request as it arrived, which is how a test sees that the live request
    was never redacted."""

    seen: list[tuple[str, str, Headers, bytes]] = []

    def _answer(self, content_type: str, body: bytes, extra: Headers = ()) -> None:
        length = int(self.headers.get("content-length") or 0)
        sent = self.rfile.read(length) if length else b""
        _RedactStandIn.seen.append((self.command, self.path, tuple(self.headers.items()), sent))
        self.send_response(200)
        self.send_header("content-type", content_type)
        for name, value in extra:
            self.send_header(name, value)
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        self._answer("application/octet-stream", BINARY_BODY, (("set-cookie", SESSION_COOKIE),))

    def do_POST(self):
        if self.path == "/v1/messages":
            self._answer("application/json", LLM_ANSWER)
        elif self.path == "/v1/stream":
            self._answer("text/event-stream", SSE_BODY)
        else:  # the webhook's target, answering as Slack does
            self._answer("text/html", b"ok")

    def log_message(self, *args):
        pass


@pytest.fixture
def redact_stand_in():
    _RedactStandIn.seen = []
    srv = ThreadingHTTPServer(("127.0.0.1", 0), _RedactStandIn)
    threading.Thread(target=lambda: srv.serve_forever(poll_interval=0.01), daemon=True).start()
    yield srv.server_address[1]
    srv.shutdown()


@dataclasses.dataclass(frozen=True)
class _Call:
    """One request for CALLS_CHILD to make. `host` "" is the reverse door: the listener's own
    authority, which only the child knows."""

    method: str
    target: str
    headers: Headers = ()
    body: bytes | None = None
    host: str = ""


@dataclasses.dataclass(frozen=True)
class _Answer:
    """What the agent was answered, as it read it off the wire."""

    status: int
    headers: Headers
    body: bytes

    def header(self, name: str) -> str | None:
        return header_value(self.headers, name)


# Makes the calls in the JSON file it is handed, in order, each through the listener, and writes
# what came back to the second file. A file and not stdout: irimi prints each exchange's line to
# the same stdout as it finishes, and a streamed one finishes as the child reads it, so the two
# interleave. `putheader` rather than a dict, as in RUN_HEADER_CHILD, so what the test lists is
# exactly what is sent.
CALLS_CHILD = """
import base64, http.client, json, os, sys, urllib.parse

proxy = urllib.parse.urlparse(os.environ["HTTP_PROXY"])
with open(sys.argv[1]) as f:
    calls = json.load(f)
answers = []
for call in calls:
    body = None if call["body"] is None else base64.b64decode(call["body"])
    conn = http.client.HTTPConnection(proxy.hostname, proxy.port, timeout=10)
    conn.putrequest(call["method"], call["target"], skip_host=True, skip_accept_encoding=True)
    conn.putheader("host", call["host"] or "127.0.0.1:%d" % proxy.port)
    for name, value in call["headers"]:
        conn.putheader(name, value)
    if body is not None:
        conn.putheader("content-length", str(len(body)))
    conn.endheaders(body)
    resp = conn.getresponse()
    data = base64.b64encode(resp.read()).decode()
    answers.append({"status": resp.status, "headers": resp.getheaders(), "body": data})
    conn.close()
with open(sys.argv[2], "w") as f:
    json.dump(answers, f)
"""


def _run_calls_under_shadow(
    tmp_path, monkeypatch, stand_in: int, calls: list[_Call], *flags: str
) -> list[_Answer]:
    """`calls` from a child of the real `irimi shadow`, over the shipped maps plus STANDIN_MAP, and
    what the child was answered for each. `flags` go before the `--`. The engine's own L3 reader
    is pointed at the stand-in, so nothing here can dial a real host even by mistake."""
    maps_dir = tmp_path / "shipped"
    shutil.copytree(loader.shipped_dir(), maps_dir)
    (maps_dir / "standin.yaml").write_text(STANDIN_MAP)
    monkeypatch.setattr("irimi.servicemap.loader.shipped_dir", lambda: maps_dir)
    monkeypatch.setattr("irimi.reader.UpstreamReader", _StandInReader)
    _StandInReader.port = stand_in
    spec = [
        {
            "method": c.method,
            "target": c.target,
            "host": c.host,
            "headers": [list(pair) for pair in c.headers],
            "body": None if c.body is None else base64.b64encode(c.body).decode(),
        }
        for c in calls
    ]
    (tmp_path / "calls.json").write_text(json.dumps(spec))
    child = tmp_path / "child.py"
    child.write_text(CALLS_CHILD)
    answers = tmp_path / "answers.json"
    argv = [*flags, "--", sys.executable, str(child), str(tmp_path / "calls.json"), str(answers)]
    assert main(["shadow", "--port", "0", *argv]) == 0
    got = json.loads(answers.read_text())
    assert len(got) == len(calls)
    return [
        _Answer(g["status"], tuple(map(tuple, g["headers"])), base64.b64decode(g["body"]))
        for g in got
    ]


def _webhook_call() -> _Call:
    """An incoming-webhook post through the reverse door, as slack_sdk's WebhookClient makes it
    once its URL is the door's."""
    return _Call(
        "POST",
        "/hooks.slack.com" + WEBHOOK_PATH,
        (("content-type", "application/json"),),
        WEBHOOK_BODY,
    )


def test_a_delegated_slack_webhook_stores_its_secret_path_as_one_placeholder_in_path_and_target(
    home, tmp_path, monkeypatch, redact_stand_in, stored
):
    """#69's credential-path rule over a real delegated write. A `hooks.slack.com/services/T/B/
    <secret>` post through the reverse door, its service pointed at a bare-origin loopback target,
    reaches that target on its own path (`delegation.target_url`), so the secret is in both the
    request path and `Exchange.target`. On disk both hold the one placeholder of the whole path and
    the target keeps its origin; the target heard, and the agent was answered, unredacted."""
    origin = f"http://127.0.0.1:{redact_stand_in}"
    (answer,) = _run_calls_under_shadow(
        tmp_path,
        monkeypatch,
        redact_stand_in,
        [_webhook_call()],
        "--target",
        f"hooks.slack.com={origin}",
    )
    assert (answer.status, answer.body, answer.header("irimi-answered-by")) == (
        200,
        b"ok",
        "delegated",
    )
    assert [(m, p, b) for m, p, _, b in _RedactStandIn.seen] == [
        ("POST", WEBHOOK_PATH, WEBHOOK_BODY)
    ]
    disk = stored()
    (ex,) = disk.received
    assert (ex.door, ex.request.host, ex.request.path, ex.answered_by, ex.target) == (
        "reverse",
        "hooks.slack.com",
        WEBHOOK_PATH,
        "delegated",
        origin + WEBHOOK_PATH,
    )

    hidden = "/" + redact.placeholder(disk.key, WEBHOOK_PATH)
    (written,) = _assert_stored_as(disk, {WEBHOOK_PATH: hidden})
    assert (written.request.path, written.target) == (hidden, origin + hidden)
    assert disk.files_holding(WEBHOOK_TOKEN.encode()) == []


def test_an_unreachable_webhook_target_leaves_its_secret_path_nowhere_on_disk(
    home, tmp_path, monkeypatch, redact_stand_in, stored
):
    """#69 over #16's failure answer. When a webhook's loopback target is not listening, irimi
    answers a 502 whose JSON names the target, and a bare-origin target's URL carries the secret
    path. The agent is told (its answer is never redacted), and the copy for disk holds the path
    nowhere: not the request, not `Exchange.target`, not the 502's body."""
    closed = socket.socket()
    closed.bind(("127.0.0.1", 0))
    dead = closed.getsockname()[1]
    closed.close()
    origin = f"http://127.0.0.1:{dead}"
    (answer,) = _run_calls_under_shadow(
        tmp_path,
        monkeypatch,
        redact_stand_in,
        [_webhook_call()],
        "--target",
        f"hooks.slack.com={origin}",
    )
    assert (answer.status, answer.header("irimi-answered-by")) == (502, "delegated")
    assert WEBHOOK_PATH.encode() in answer.body
    disk = stored()
    (ex,) = disk.received
    assert TARGET_FAILED_FLAG in ex.flags and ex.target == origin + WEBHOOK_PATH

    hidden = "/" + redact.placeholder(disk.key, WEBHOOK_PATH)
    (written,) = disk.exchanges()
    _assert_same(written, redact.redact_exchange(ex, disk.key))
    assert (written.request.path, written.target) == (hidden, origin + hidden)
    assert disk.files_holding(WEBHOOK_TOKEN.encode()) == []
    # The 502 still names its target, as the one placeholder the path and the target hold.
    assert ex.response is not None and written.response is not None
    told = json.loads(ex.response.body)["error"]["message"]
    assert json.loads(written.response.body)["error"]["message"] == told.replace(
        WEBHOOK_PATH, hidden
    )


def test_a_stripe_form_write_through_the_door_stores_its_card_token_and_query_key_as_placeholders(
    home, tmp_path, monkeypatch, redact_stand_in, stored
):
    """#69's secret-key rule over a real faked write. A Stripe form post through the reverse door
    with `card[token]=…&amount=100` and `?api_key=…` stores the token (by its last bracket
    segment) and the query key (by its name) as placeholders, and the bearer header as one;
    `amount=100` and every other byte survive. `/v1/payment_methods` is a route the shipped map
    does not list, so irimi answers with the L0 echo, which reflects the token back as JSON
    (`{"card": {"token": …}}`): the one token is one placeholder in the form and in the answer.
    The agent's own answer is irimi's, unredacted."""
    (answer,) = _run_calls_under_shadow(
        tmp_path,
        monkeypatch,
        redact_stand_in,
        [
            _Call(
                "POST",
                f"/api.stripe.com/v1/payment_methods?api_key={QUERY_API_KEY}",
                (("authorization", STRIPE_BEARER), ("content-type", FORM_CT)),
                STRIPE_FORM,
            )
        ],
    )
    assert (answer.status, answer.header("irimi-answered-by")) == (200, "fake-L0")
    assert json.loads(answer.body)["card"] == {"token": CARD_TOKEN}
    assert _RedactStandIn.seen == []  # a write is never forwarded, and it read nothing first
    disk = stored()
    (ex,) = disk.received
    assert (ex.door, ex.request.host, ex.kind, ex.answered_by, ex.flags, ex.request.body) == (
        "reverse",
        "api.stripe.com",
        "unknown",
        "fake-L0",
        (UNCLASSIFIED_FLAG, FIDELITY_L0_FLAG),
        STRIPE_FORM,
    )

    key = disk.key
    (written,) = _assert_stored_as(
        disk, _placeholders(key, STRIPE_BEARER, QUERY_API_KEY, CARD_TOKEN)
    )
    token = redact.placeholder(key, CARD_TOKEN)
    assert written.request.body == f"card[token]={token}&amount=100".encode()
    assert written.request.query == f"api_key={redact.placeholder(key, QUERY_API_KEY)}"
    assert written.request.header("authorization") == redact.placeholder(key, STRIPE_BEARER)
    assert written.response is not None
    assert json.loads(written.response.body)["card"] == {"token": token}
    assert json.loads(written.response.body)["amount"] == 100


def test_an_llm_request_stores_a_nested_live_key_and_a_token_key_as_placeholders_not_max_tokens(
    home, tmp_path, monkeypatch, redact_stand_in, stored
):
    """#69's JSON rules over a real live LLM call through the forward proxy. An `sk_live_` key three
    objects deep and a whole `token` key are placeholders on disk, `max_tokens` is not, and the
    `x-api-key` header is one placeholder. The model's answer echoes the nested key, and it is
    stored as the same placeholder as the request's. The stand-in heard, and the agent was
    answered, unredacted - a live forward carries no `Irimi-Answered-By`."""
    authority = f"127.0.0.1:{redact_stand_in}"
    (answer,) = _run_calls_under_shadow(
        tmp_path,
        monkeypatch,
        redact_stand_in,
        [
            _Call(
                "POST",
                f"http://{authority}/v1/messages",
                (("x-api-key", LLM_API_KEY), ("content-type", "application/json")),
                LLM_BODY,
                authority,
            )
        ],
    )
    assert (answer.status, answer.body, answer.header("irimi-answered-by")) == (
        200,
        LLM_ANSWER,
        None,
    )
    ((method, path, heard, sent),) = _RedactStandIn.seen
    assert (method, path, sent, header_value(heard, "x-api-key")) == (
        "POST",
        "/v1/messages",
        LLM_BODY,
        LLM_API_KEY,
    )
    disk = stored()
    (ex,) = disk.received
    assert (ex.kind, ex.answered_by, ex.request.body) == ("llm", "live", LLM_BODY)

    key = disk.key
    hidden = _placeholders(key, LLM_API_KEY, NESTED_KEY, LLM_TOKEN)
    (written,) = _assert_stored_as(disk, hidden)
    assert json.loads(written.request.body)["max_tokens"] == 1024
    nested = redact.placeholder(key, NESTED_KEY)
    assert json.loads(written.request.body)["metadata"] == {"agent": {"env": {"key": nested}}}
    assert written.response is not None and nested.encode() in written.response.body


def test_a_streamed_sse_answer_is_stored_whole_with_each_live_key_as_its_placeholder(
    home, tmp_path, monkeypatch, redact_stand_in, stored
):
    """#69's text rule over a real `text/event-stream` answer, recorded by #71. The agent reads
    the whole stream unredacted, both keys included. The engine streams it through and records
    the chunks it sent, and the run's own store holds that body with each key as its placeholder:
    the second key too, which follows a JSON `\\n` escape inside the data line."""
    authority = f"127.0.0.1:{redact_stand_in}"
    (answer,) = _run_calls_under_shadow(
        tmp_path,
        monkeypatch,
        redact_stand_in,
        [
            _Call(
                "POST",
                f"http://{authority}/v1/stream",
                (("content-type", "application/json"),),
                b"{}",
                authority,
            )
        ],
    )
    assert (answer.status, answer.body, answer.header("irimi-answered-by")) == (
        200,
        SSE_BODY,
        None,
    )
    disk = stored()
    (ex,) = disk.received
    assert (ex.kind, ex.answered_by, ex.flags) == ("llm", "live", ())
    assert ex.response is not None and ex.response.body == SSE_BODY
    assert ex.stream_chunks and sum(ex.stream_chunks) == len(SSE_BODY)

    hidden = _placeholders(disk.key, SSE_PLAIN_KEY, SSE_ESCAPED_KEY)
    (written,) = _assert_stored_as(disk, hidden)
    assert written.response is not None
    assert sum(written.stream_chunks) == len(written.response.body)
    for secret in hidden.values():
        assert secret.encode() in written.response.body


def test_a_live_reads_set_cookie_is_stored_as_a_placeholder_and_its_binary_body_unchanged(
    home, tmp_path, monkeypatch, redact_stand_in, stored
):
    """#69's response-header rule and its binary limitation over a real live read. The response's
    `Set-Cookie` is one placeholder on disk; its body is not UTF-8, so it is stored unscanned and
    byte for byte. The agent got the cookie and the body as the stand-in sent them."""
    authority = f"127.0.0.1:{redact_stand_in}"
    (answer,) = _run_calls_under_shadow(
        tmp_path,
        monkeypatch,
        redact_stand_in,
        [_Call("GET", f"http://{authority}/v1/session", host=authority)],
    )
    assert (answer.status, answer.body, answer.header("set-cookie")) == (
        200,
        BINARY_BODY,
        SESSION_COOKIE,
    )
    assert answer.header("irimi-answered-by") is None
    assert [(m, p) for m, p, _, _ in _RedactStandIn.seen] == [("GET", "/v1/session")]
    disk = stored()
    (ex,) = disk.received
    assert (ex.kind, ex.answered_by) == ("read", "live")

    (written,) = _assert_stored_as(disk, _placeholders(disk.key, SESSION_COOKIE))
    assert written.response is not None
    assert written.response.header("set-cookie") == redact.placeholder(disk.key, SESSION_COOKIE)
    assert written.response.body == BINARY_BODY
    assert disk.blobs()[trace.body_ref(BINARY_BODY).sha256] == BINARY_BODY
