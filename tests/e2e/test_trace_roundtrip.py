"""What `irimi shadow` stores, read back off disk: each exchange redacted, its other fields intact.

`_SpyStore` is the real store, also remembering each exchange it was handed."""

import base64
import dataclasses
import json
import shutil
import socket
import sys
import time
from bisect import bisect_left
from itertools import accumulate, pairwise
from pathlib import Path

import pytest

from irimi.cli import main
from irimi.core.bodies import FORM_CT
from irimi.core.exchange import Exchange, Flag, Headers, Precondition, header_value, media_type
from irimi.protocol import trace
from irimi.protocol.env import RUN_ENV
from irimi.protocol.trace import StoredRun, trigger_args
from irimi.record import redact
from irimi.record.store.reader import StoreReader
from irimi.record.store.writer import DirectoryStore
from irimi.report.terminal import exchange_line
from irimi.servicemaps import loader
from tests.support.credentials import (
    AGENT_STRIPE_KEY,
    E2E_LLM_KEY,
    E2E_NESTED_KEY,
    E2E_SSE_ESCAPED_KEY,
    E2E_SSE_PLAIN_KEY,
    E2E_STRIPE_KEY,
)
from tests.support.engine import carries_run_header
from tests.support.scenarios import (
    BASE_URL_CHILD,
    CHARGE_ID,
    REFUND_AMOUNT,
    REFUND_AMOUNT_MINOR,
    REFUND_CHARGE,
    StandInReader,
    run_refund_scenario,
)
from tests.support.upstreams import Logged, Port, serve

pytestmark = pytest.mark.e2e

# The fields the live summary prints from: a stored exchange must carry each, so a summary
# printed from the store says what the live one said.
REFUND_FIELDS = (
    "precondition.outcome",
    "precondition.rejection_code",
    "overlay",
    "issued_by",
    "would_fire",
    "precondition.currency",
)


class _SpyStore(DirectoryStore):
    """The real store, also remembering each exchange the engine handed it, in order."""

    built: list["_SpyStore"] = []

    def __init__(self, root: Path, key: bytes, on_run_ended=None, replay_of=None) -> None:
        super().__init__(root, key, on_run_ended, replay_of)
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
        """Return the one run `irimi shadow` started for its child."""
        (record,) = [r for r in self.reader.list_runs() if r.attribution == "process"]
        return self.reader.load_run(record.run_id)

    def exchanges(self) -> list[Exchange]:
        """Return every stored exchange, run by run in `seq` order, then the unattributed ones."""
        ids = [r.run_id for r in self.reader.list_runs()] + [trace.UNATTRIBUTED]
        runs = [self.reader.load_run(run_id) for run_id in ids]
        return [event for run in runs for event in run.events if isinstance(event, Exchange)]

    def lines(self, run_id: str) -> list[str]:
        return (self.root / "runs" / run_id / "events.jsonl").read_text().splitlines()

    def blobs(self) -> dict[str, bytes]:
        blobs = self.root / "blobs"
        return {path.name: path.read_bytes() for path in blobs.iterdir()} if blobs.is_dir() else {}

    def files_holding(self, needle: bytes) -> list[Path]:
        """Return every file under the root whose bytes hold `needle`."""
        return [p for p in self.root.rglob("*") if p.is_file() and needle in p.read_bytes()]


@pytest.fixture
def stored(home, monkeypatch):
    """Return a function giving the run's store, read back after `main` returned.

    It checks the store dropped nothing, has nothing queued, and was handed exactly the exchanges
    `irimi shadow` printed, in order."""
    _SpyStore.built = []
    monkeypatch.setattr("irimi.record.store.writer.DirectoryStore", _SpyStore)
    printed: list[Exchange] = []
    line = exchange_line

    def printing(ex: Exchange) -> str:
        printed.append(ex)
        return line(ex)

    monkeypatch.setattr("irimi.report.terminal.exchange_line", printing)

    def the_disk() -> _Disk:
        (store,) = _SpyStore.built
        assert (store.stats().queued, store.stats().dropped) == (0, 0)
        assert len(store.received) == len(printed)
        assert all(a is b for a, b in zip(store.received, printed, strict=True))
        return _Disk(store.layout.root, redact.load_key(home), store.received)

    return the_disk


def _assert_same(stored: Exchange, expected: Exchange) -> None:
    """Compare two exchanges field by field, so a failure names the field."""
    for field in dataclasses.fields(Exchange):
        assert getattr(stored, field.name) == getattr(expected, field.name), field.name


def _decoded(disk: _Disk) -> list[Exchange]:
    """Return the process run's stored exchanges, checking each against its live one, redacted.

    `seq` must run 1..n with no gap."""
    run = disk.process_run()
    seqs = [json.loads(line)["seq"] for line in disk.lines(run.record.run_id)]
    assert seqs == list(range(1, len(disk.received) + 1))
    decoded = [event for event in run.events if isinstance(event, Exchange)]
    for stored, live in zip(decoded, disk.received, strict=True):
        _assert_same(stored, redact.redact_exchange(live, disk.key))
    return decoded


def test_the_refund_scenario_survives_the_trace_format_event_for_event(
    home, tmp_path, monkeypatch, stored
):
    """The refund scenario's run reads back off disk equal to what the engine stored, redacted.

    It leaves one process run with its argv and `outcome: ok`, holding the agent's five calls and
    irimi's two check reads, and every summary field arrives with its live value."""
    root = tmp_path / "trace"
    run_refund_scenario(tmp_path, monkeypatch, "--store", str(root))

    disk = stored()
    assert disk.root == root
    run = disk.process_run()
    assert [path.name for path in (root / "runs").iterdir()] == [run.record.run_id]
    assert run.record.attribution == "process"
    assert run.record.trigger is not None
    assert trigger_args(run.record.trigger) == {
        "argv": [sys.executable, str(tmp_path / "child.py")]
    }
    assert (run.record.end.outcome, run.record.end.exit_code, run.record.end.error) == (
        "ok",
        0,
        None,
    )
    assert run.record.dropped_events == 0
    decoded = _decoded(disk)
    shape = [(ex.issued_by, ex.request.method, ex.request.path, ex.answered_by) for ex in decoded]
    assert shape == [
        ("agent", "GET", "/v1/charges", "live"),
        ("engine", "GET", f"/v1/charges/{REFUND_CHARGE}", "live"),
        ("agent", "POST", "/v1/refunds", "fake-L1"),
        ("agent", "GET", "/v1/refunds", "overlay"),
        ("agent", "GET", f"/v1/charges/{REFUND_CHARGE}", "overlay"),
        ("engine", "GET", f"/v1/charges/{REFUND_CHARGE}", "live"),
        ("agent", "POST", "/v1/refunds", "fake-L1"),
    ]
    _, check, refund, refunds, charge, recheck, retry = decoded
    assert refund.precondition == Precondition("passed", "", "usd")
    assert refund.would_fire == ("refund.created", "charge.refunded")
    assert retry.precondition == Precondition("rejected", "charge_already_refunded", "usd")
    assert retry.would_fire == ()
    assert (refunds.overlay, charge.overlay) == ("full", "full")
    assert (check.issued_by, recheck.issued_by) == ("engine", "engine")
    defaults = {f.name: f.default for f in dataclasses.fields(Exchange)}
    for name in REFUND_FIELDS:
        head, _, part = name.partition(".")
        default = getattr(defaults[head], part) if part else defaults[head]
        values = [getattr(ex, head) for ex in decoded]
        values = [getattr(v, part) for v in values] if part else values
        assert any(value != default for value in values), name

    # Bodies never go inline: the refund's form body is a ref in the line and a blob beside it,
    # and the retry posted the same bytes, so both lines name one blob.
    posted = f"charge={REFUND_CHARGE}&amount={REFUND_AMOUNT}".encode()
    lines = disk.lines(run.record.run_id)
    assert not any(posted.decode() in line for line in lines)
    assert disk.blobs()[trace.body_ref(posted).sha256] == posted
    refund_line, retry_line = (json.loads(lines[i]) for i in (2, 6))
    assert refund_line["request"]["body"] == retry_line["request"]["body"]


def test_every_exchange_the_refund_scenario_stores_is_timed_in_completion_order(
    home, tmp_path, monkeypatch, stored
):
    """Stored exchanges are timed inside their run, and `seq` is completion order.

    Each irimi check read sits inside the write it checked, so it is stored first though the write
    started first: `started_at` recovers start order."""
    before = time.time()
    run_refund_scenario(tmp_path, monkeypatch)
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
    assert record.started_at is not None and record.end.ended_at is not None
    assert before <= record.started_at <= read.started_at
    assert retry.ended_at <= record.end.ended_at <= after


# Each call names itself in its query, so the recorded exchange can be matched to what it sent.
# `putheader` rather than a dict, because a dict cannot send one header twice.
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
    """An `Irimi-Run` value that is no valid run id names no run, and no service sees the header.

    A run id becomes a store directory, so `../x`, 65 characters or a space leave the exchange in
    the engine's run. A valid id names its run in any header spelling, trimmed, the first valid
    value of a repeated header winning; nothing is created outside the store's root."""
    child = tmp_path / "child.py"
    child.write_text(
        RUN_HEADER_CHILD.replace("__PORT__", str(upstream)).replace("__RUN_ENV__", RUN_ENV)
    )
    assert main(["shadow", "--port", "0", "--", sys.executable, str(child)]) == 0
    out = capfd.readouterr().out.splitlines()
    (engine_run,) = [line.split()[1] for line in out if line.startswith("RUN ")]
    assert f"irimi shadow · run {engine_run} · listening on" in "\n".join(out)
    assert trace.is_valid_run_id(engine_run)
    assert len([line for line in out if line.startswith("CALL ") and line.endswith(" 200")]) == 8

    assert len(upstream.seen) == 7  # the seven GETs; the POST was faked and never left
    for path, received in upstream.seen:
        assert not carries_run_header(received), f"{path} reached the service carrying Irimi-Run"
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
        assert not carries_run_header(ex.request.headers)
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


def test_the_base_url_refund_survives_the_trace_format(
    home, tmp_path, monkeypatch, stripe_stand_in, stored
):
    """A proxied read and a base-URL refund, faked from the shipped map's fixture, store intact.

    Each exchange decodes equal and is timed, the refund's `door: reverse` included, and irimi's
    check read sits inside the refund's span."""
    _run_base_url_refund(tmp_path, monkeypatch, stripe_stand_in)

    read, check, refund = _decoded(stored())
    assert [(ex.door, ex.issued_by, ex.kind) for ex in (read, check, refund)] == [
        ("forward", "agent", "read"),
        ("forward", "engine", "read"),
        ("reverse", "agent", "write"),
    ]
    assert (refund.request.host, refund.answered_by, refund.precondition.currency) == (
        "api.stripe.com",
        "fake-L1",
        "usd",
    )
    assert refund.precondition.outcome == "passed"
    for ex in (read, check, refund):
        assert 0 < ex.started_at <= ex.ended_at
    assert refund.started_at < check.started_at <= check.ended_at <= refund.ended_at


def _run_base_url_refund(tmp_path, monkeypatch, stripe_port: Port) -> None:
    """Run `BASE_URL_CHILD` under `irimi shadow` against the loopback Stripe, which hears no write.

    Its calls carry `AGENT_STRIPE_KEY` as their bearer token."""
    monkeypatch.setattr("irimi.decide.reader.UpstreamReader", StandInReader)
    StandInReader.port = stripe_port
    child = tmp_path / "child.py"
    child.write_text(
        BASE_URL_CHILD.replace("__CHARGES_PORT__", str(stripe_port))
        .replace("__CHARGE__", CHARGE_ID)
        .replace("__AMOUNT__", str(REFUND_AMOUNT_MINOR))
    )
    assert main(["shadow", "--port", "0", "--", sys.executable, str(child)]) == 0
    assert [m for m, _ in stripe_port.seen if m != "GET"] == []


def test_the_base_url_refund_redacted_for_disk_keeps_no_credential(
    home, tmp_path, monkeypatch, stripe_stand_in, stored
):
    """No stored file holds `sk_test_`, and the bearer token is one placeholder wherever it went.

    That covers irimi's check read too. Swapping the token for its placeholder in what the engine
    recorded gives exactly what was stored."""
    _run_base_url_refund(tmp_path, monkeypatch, stripe_stand_in)
    disk = stored()
    bearer = f"Bearer {AGENT_STRIPE_KEY}"
    written = _assert_stored_as(disk, _placeholders(disk.key, bearer))
    assert disk.files_holding(b"sk_test_") == []
    sent = [ex.request.header("authorization") for ex in written]
    assert set(sent) == {redact.placeholder(disk.key, bearer)}, sent
    assert disk.received[0].request.header("authorization") == bearer


# ------------------------------------------------------------ redaction over real traffic
#
# Each redaction rule over real `irimi shadow` traffic, proxied or base-URL. `_assert_stored_as`
# checks no secret is on disk and every other byte survives. Every upstream is loopback.


def _placeholders(key: bytes, *secrets: str) -> dict[str, str]:
    """Map each secret to the placeholder it must be stored as."""
    return {secret: redact.placeholder(key, secret) for secret in secrets}


def _with_placeholders(ex: Exchange, hidden: dict[str, str]) -> Exchange:
    """Return `ex` with each secret in `hidden` swapped for its placeholder wherever `redact` reads.

    Longest secret first, so a value holding a shorter secret is swapped whole. A changed JSON body
    is re-serialized compact; any other body keeps its bytes, and a stream's chunks follow it."""
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
    """Return a stream's chunks after redaction, by a rule stated apart from `redact`'s own.

    A boundary in an unchanged line keeps its place, one in a changed line moves to that line's
    end, and boundaries that meet make one chunk."""
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
    """Check no secret in `hidden` is on disk and each stored exchange is its live one, swapped.

    Returns the stored exchanges."""
    for secret in hidden:
        assert disk.files_holding(secret.encode()) == [], secret
    written = disk.exchanges()
    for live, stored in zip(disk.received, written, strict=True):
        _assert_same(stored, _with_placeholders(live, hidden))
    return written


# The stub's own secrets and answers. Each is a value no rule would touch unless it is one.
WEBHOOK_TOKEN = "xq9WebhookPathSecret24"  # a real one is 24 alphanumerics, no credential shape
WEBHOOK_PATH = f"/services/T0REDACT/B0REDACT/{WEBHOOK_TOKEN}"
WEBHOOK_BODY = b'{"text":"deploy finished"}'
QUERY_API_KEY = "qs-secret-api-key-4f2"  # a secret by its name only, like CARD_TOKEN
CARD_TOKEN = "tok_redact_e2e"
STRIPE_BEARER = f"Bearer {E2E_STRIPE_KEY}"
STRIPE_FORM = f"card[token]={CARD_TOKEN}&amount=100".encode()
LLM_API_KEY = E2E_LLM_KEY
NESTED_KEY = E2E_NESTED_KEY
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
SSE_PLAIN_KEY = E2E_SSE_PLAIN_KEY
SSE_ESCAPED_KEY = E2E_SSE_ESCAPED_KEY
# The second event's key follows a JSON `\n` escape: a backslash and an `n`, as the wire has it.
SSE_BODY = (
    b'data: {"text":"key: ' + SSE_PLAIN_KEY.encode() + b'"}\n\n'
    b'data: {"text":"key:\\n' + SSE_ESCAPED_KEY.encode() + b'"}\n\n'
)
# Chunk cuts inside the first key, after the blank line, and inside the second key, so both
# boundary rules in `_chunks_after` are exercised.
SSE_CUTS = (
    SSE_BODY.index(b"StreamedPlain"),
    SSE_BODY.index(b"\n\n") + 2,
    SSE_BODY.index(b"AfterAnEscape"),
)
# How long the stub waits between chunks, so the proxy reads each one on its own.
SSE_CHUNK_GAP_S = 0.05
SESSION_COOKIE = "session=cookie-secret-77; Path=/; HttpOnly"
BINARY_BODY = b"\x89PNG\r\n\x1a\n\xff\xfe\x00 not UTF-8, stored as it came"

# The stub's host as a service, so its routes classify. It sits beside the shipped maps, which the
# Slack webhook and the Stripe write need.
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


class _RedactStandIn(Logged):
    """The upstream and webhook target in the redaction runs, recording each request as it came.

    That record shows the live request was never redacted."""

    def _answer(
        self, content_type: str, body: bytes, extra: Headers = (), cuts: tuple[int, ...] = ()
    ) -> None:
        """Answer `body`, written in pieces cut at `cuts`, SSE_CHUNK_GAP_S apart."""
        length = int(self.headers.get("content-length") or 0)
        sent = self.rfile.read(length) if length else b""
        self.seen.append((self.command, self.path, tuple(self.headers.items()), sent))
        self.send_response(200)
        self.send_header("content-type", content_type)
        for name, value in extra:
            self.send_header(name, value)
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        for start, end in pairwise((0, *cuts, len(body))):
            if start:
                time.sleep(SSE_CHUNK_GAP_S)
            self.wfile.write(body[start:end])

    def do_GET(self):
        self._answer("application/octet-stream", BINARY_BODY, (("set-cookie", SESSION_COOKIE),))

    def do_POST(self):
        if self.path == "/v1/messages":
            self._answer("application/json", LLM_ANSWER)
        elif self.path == "/v1/stream":
            self._answer("text/event-stream", SSE_BODY, cuts=SSE_CUTS)
        else:  # the webhook's target, answering as Slack does
            self._answer("text/html", b"ok")


@pytest.fixture
def redact_stand_in():
    port = serve(_RedactStandIn, threaded=True)
    yield port
    port.stop()


@dataclasses.dataclass(frozen=True)
class _Call:
    """One request for CALLS_CHILD to make; `host` "" sends it in base-URL mode, to the listener."""

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


# Makes the calls in its first file through the listener and writes the answers to its second.
# A file, not stdout: irimi's own lines interleave with a stream the child is still reading.
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
    """Make `calls` from a child of `irimi shadow` and return what it was answered for each.

    The maps are the shipped ones plus STANDIN_MAP, and `flags` go before the `--`. irimi's check
    reads go to the stub, so nothing can dial a real host."""
    maps_dir = tmp_path / "shipped"
    shutil.copytree(loader.shipped_dir(), maps_dir)
    (maps_dir / "standin.yaml").write_text(STANDIN_MAP)
    monkeypatch.setattr("irimi.servicemaps.loader.shipped_dir", lambda: maps_dir)
    monkeypatch.setattr("irimi.decide.reader.UpstreamReader", StandInReader)
    StandInReader.port = stand_in
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
    """Return a base-URL Slack webhook post, as slack_sdk's WebhookClient makes it."""
    return _Call(
        "POST",
        "/hooks.slack.com" + WEBHOOK_PATH,
        (("content-type", "application/json"),),
        WEBHOOK_BODY,
    )


def test_a_delegated_slack_webhook_stores_its_secret_path_as_one_placeholder_in_path_and_target(
    home, tmp_path, monkeypatch, redact_stand_in, stored
):
    """A webhook sent to a target stores its secret path as one placeholder in path and target.

    A bare-origin target gets the request's own path, so the secret is in both. On disk the target
    keeps its origin; the target heard, and the agent was answered, unredacted."""
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
    assert [(m, p, b) for m, p, _, b in redact_stand_in.seen] == [
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
    """irimi's 502 for an unreachable webhook target names the target, but not on disk.

    The agent's answer holds the secret path; the stored request, target and 502 body do not."""
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
    assert (answer.status, answer.header("irimi-answered-by")) == (502, "irimi")
    assert WEBHOOK_PATH.encode() in answer.body
    disk = stored()
    (ex,) = disk.received
    assert Flag.TARGET_FAILED in ex.flags and ex.target == origin + WEBHOOK_PATH

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
    """A faked form write stores its card token, query key and bearer as placeholders.

    The token is found by its last bracket segment, the key by its name; every other byte
    survives. The unlisted route gets the echo, which reflects the token back, so the one token is
    one placeholder in the form and in the answer. The agent's answer is unredacted."""
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
    assert redact_stand_in.seen == []  # a write is never forwarded, and it read nothing first
    disk = stored()
    (ex,) = disk.received
    assert (ex.door, ex.request.host, ex.kind, ex.answered_by, ex.flags, ex.request.body) == (
        "reverse",
        "api.stripe.com",
        "unknown",
        "fake-L0",
        (Flag.UNCLASSIFIED,),
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
    """A live LLM call stores a nested `sk_live_` key and a `token` key as placeholders.

    `max_tokens` is kept and `x-api-key` is one placeholder. The answer echoes the nested key and
    stores the same placeholder. The stub heard, and the agent got, everything unredacted."""
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
    ((method, path, heard, sent),) = redact_stand_in.seen
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
    """A recorded event stream is stored whole, each key as its placeholder.

    The agent reads it unredacted. The second key follows a JSON `\\n` escape in its data line."""
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
    # The proxy may read two of the stub's pieces as one; more than one chunk is what it pins.
    assert len(ex.stream_chunks) > 1 and sum(ex.stream_chunks) == len(SSE_BODY)

    hidden = _placeholders(disk.key, SSE_PLAIN_KEY, SSE_ESCAPED_KEY)
    (written,) = _assert_stored_as(disk, hidden)
    assert written.response is not None
    assert sum(written.stream_chunks) == len(written.response.body)
    for secret in hidden.values():
        assert secret.encode() in written.response.body


def test_a_live_reads_set_cookie_is_stored_as_a_placeholder_and_its_binary_body_unchanged(
    home, tmp_path, monkeypatch, redact_stand_in, stored
):
    """A live read's `Set-Cookie` is stored as a placeholder, its non-UTF-8 body byte for byte.

    The agent got the cookie and the body as the stub sent them."""
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
    assert [(m, p) for m, p, _, _ in redact_stand_in.seen] == [("GET", "/v1/session")]
    disk = stored()
    (ex,) = disk.received
    assert (ex.kind, ex.answered_by) == ("read", "live")

    (written,) = _assert_stored_as(disk, _placeholders(disk.key, SESSION_COOKIE))
    assert written.response is not None
    assert written.response.header("set-cookie") == redact.placeholder(disk.key, SESSION_COOKIE)
    assert written.response.body == BINARY_BODY
    assert disk.blobs()[trace.body_ref(BINARY_BODY).sha256] == BINARY_BODY
