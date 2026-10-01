"""Reading a stored run back: its summary, `irimi runs list` and `irimi runs show` (#72).

The claim this issue exists for is that the store holds everything the live summary prints. The
first test proves it the only way that counts: the Phase 2 exit scenario, plus one telemetry call,
runs under the real `irimi shadow`, and the summary printed from what it stored is the block the
CLI printed on exit, line for line, but for the elapsed seconds. The rest pin the two commands'
exact output over stores built by `irimi shadow` and by hand.
"""

import dataclasses
import json
import re
import sys
import time
from pathlib import Path

import pytest

from irimi import redact, report, servicemap, trace
from irimi.cli import DEFAULT_RUNS_LIMIT, main
from irimi.exchange import Exchange, Request, Response
from irimi.store import DirectoryStore, StoreReader
from irimi.trace import ErrorInfo, RunRecord, ToolCall, Trigger
from tests import test_phase_exit
from tests.test_engine_mitm import PRECONDITION_STRIPE_MAP
from tests.test_phase_exit import PHASE2_SUMMARY, _run_phase2_under_shadow

home = test_phase_exit.home  # bound here so pytest finds it

# The loopback Stripe's map with one telemetry route on it. A GET, because the stand-in fails the
# helper on any other verb it hears; telemetry is forwarded live, so the stand-in answers it.
TELEMETRY_ROUTE = """  - match:
      method: GET
      path: /v1/telemetry
    operation: telemetry.send
    kind: telemetry
"""
STRIPE_WITH_TELEMETRY = PRECONDITION_STRIPE_MAP.replace("routes:\n", "routes:\n" + TELEMETRY_ROUTE)
ELAPSED = re.compile(r" · \d+\.\ds · ")


def _summary_block(lines: list[str]) -> list[str]:
    """The live summary, from its `N exchanges` header to the end of what the CLI printed."""
    header = next(i for i, line in enumerate(lines) if re.search(r" · \d+ exchanges? · ", line))
    return lines[header:]


def _no_elapsed(lines: list[str]) -> list[str]:
    return [ELAPSED.sub(" · <elapsed> · ", line) for line in lines]


def _process_run(store) -> RunRecord:
    (run,) = [r for r in StoreReader(store).list_runs() if r.attribution == "process"]
    return run


def test_the_phase_2_run_prints_the_same_summary_from_its_store_as_it_did_live(
    home, tmp_path, capfd, monkeypatch
):
    """#72's invariant. The only field allowed to differ is the elapsed seconds: the live summary
    times the child, and the stored one the run record's own start and end."""
    _run_phase2_under_shadow(
        tmp_path,
        monkeypatch,
        stripe_map=STRIPE_WITH_TELEMETRY,
        calls='call("GET", "/v1/telemetry")\n',
    )
    live = _summary_block(capfd.readouterr().out.splitlines())
    # The Phase 2 block, with the telemetry call on a line of its own and in the counts.
    assert live[1:] == [
        *PHASE2_SUMMARY[:2],
        "  telemetry  1 exchange to 1 host, forwarded live",
        *PHASE2_SUMMARY[2:-2],
        "  8 exchanges · 6 live · 0 delegated · 2 virtualized",
        PHASE2_SUMMARY[-1],
    ]

    store = home / "store"
    run = StoreReader(store).load_run(_process_run(store).run_id)
    assert [type(e).__name__ for e in run.events].count("TelemetrySeen") == 1
    stored = report.stored_summary_lines(run, servicemap.load())
    assert _no_elapsed(stored) == _no_elapsed(live)

    # And `runs show` ends in that same block, maps loaded as `shadow` loaded them.
    assert main(["runs", "show", run.record.run_id]) == 0
    shown = capfd.readouterr().out.splitlines()
    assert _no_elapsed(shown[-len(live) :]) == _no_elapsed(live)


# ------------------------------------------------------------------------------ runs list


def _shadow(capfd, *code: str) -> str:
    """`irimi shadow` over a child that makes no request; its process run's id."""
    main(["shadow", "--port", "0", "--", sys.executable, "-c", "\n".join(code)])
    banner = next(
        line
        for line in capfd.readouterr().out.splitlines()
        if line.startswith("irimi shadow · run ")
    )
    return banner.split(" · ")[1].removeprefix("run ")


STARTED = r"\d{4}-\d\d-\d\d \d\d:\d\d:\d\d"
HAND_BUILT_AT = 1_700_000_000.0


def _hand_built_incomplete_run(store, key) -> None:
    """A run that started and never ended, as a crashed irimi leaves one: one faked write, one
    tool call and one telemetry exchange in it."""
    s = DirectoryStore(store, key)
    s.start_run(
        RunRecord(
            schema_version=trace.SCHEMA_VERSION,
            run_id="handbuilt",
            mode="shadow",
            attribution="sdk",
            trigger=Trigger("refund_ticket", "agent:refund_ticket", {"ticket": "T-1"}, True),
            agent_version="2.0.1",
            engine_version="0.0.0",
            sdk_version="0.0.0",
            started_at=HAND_BUILT_AT,
            ended_at=None,
            outcome=None,
            error=None,
            exit_code=None,
        )
    )
    s.record(_refund("handbuilt"))
    s.record_tool_call(
        ToolCall(
            "tc1",
            "handbuilt",
            "db.mark_refunded",
            "write",
            "shadow",
            {"order": 7},
            None,
            ErrorInfo("db.Locked", "database is locked"),
            HAND_BUILT_AT + 1,
            HAND_BUILT_AT + 2,
        )
    )
    s.record(_telemetry("handbuilt"))
    s.close()


def _refund(run_id: str) -> Exchange:
    return Exchange(
        request=Request(
            "POST",
            "https",
            "api.stripe.com",
            443,
            "/v1/refunds",
            "",
            (("content-type", "application/x-www-form-urlencoded"),),
            b"charge=ch_1&amount=4900&currency=usd",
        ),
        response=Response(200, (), b"{}"),
        service="stripe",
        operation="refunds.create",
        kind="write",
        answered_by="fake-L0",
        validation="unvalidated",
        run_id=run_id,
        started_at=HAND_BUILT_AT,
        ended_at=HAND_BUILT_AT + 0.5,
    )


def _telemetry(run_id: str) -> Exchange:
    return Exchange(
        request=Request("POST", "https", "api.smith.langchain.com", 443, "/runs", "", (), b"{}"),
        response=Response(202, (), b""),
        service="langsmith",
        operation="runs.create",
        kind="telemetry",
        answered_by="live",
        validation="unvalidated",
        run_id=run_id,
        started_at=HAND_BUILT_AT + 3,
        ended_at=HAND_BUILT_AT + 3.5,
    )


def _local(at: float) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(at))


def test_runs_list_prints_an_ok_an_error_and_an_incomplete_run_newest_first(home, capfd):
    ok = _shadow(capfd, "pass")
    error = _shadow(capfd, "import sys", "sys.exit(3)")
    _hand_built_incomplete_run(home / "store", redact.load_key(home))
    capfd.readouterr()

    assert main(["runs", "list"]) == 0
    *shadowed, handbuilt = capfd.readouterr().out.splitlines()
    python = sys.executable
    assert [re.sub(STARTED, "<started>", re.sub(r"\d+\.\ds", "<dur>", ln)) for ln in shadowed] == [
        f"{error}  <started>  <dur>  error  process  {python}  0 exchanges  0 writes",
        f"{ok}  <started>  <dur>  ok  process  {python}  0 exchanges  0 writes",
    ]
    assert handbuilt == (
        f"handbuilt  {_local(HAND_BUILT_AT)}  -  incomplete  sdk  refund_ticket  "
        "2 exchanges  1 writes"
    )

    assert main(["runs", "list", "--limit", "1"]) == 0
    assert [line.split()[0] for line in capfd.readouterr().out.splitlines()] == [error]


def test_runs_list_over_a_missing_or_empty_store_says_so_and_succeeds(home, capfd):
    store = home / "elsewhere"
    assert main(["runs", "list", "--store", str(store)]) == 0
    assert capfd.readouterr().out == f"no runs in {store}\n"
    (store / "runs").mkdir(parents=True)
    assert main(["runs", "list", "--store", str(store)]) == 0
    assert capfd.readouterr().out == f"no runs in {store}\n"


@pytest.mark.parametrize("limit", ["0", "-1", "two"])
def test_runs_list_refuses_a_limit_that_is_not_a_positive_number(limit, capsys):
    with pytest.raises(SystemExit):
        main(["runs", "list", "--limit", limit])
    assert "at least 1" in capsys.readouterr().err


def test_runs_list_names_a_damaged_run_and_still_lists_the_others(home, capfd):
    _hand_built_incomplete_run(home / "store", redact.load_key(home))
    for blob in (home / "store" / "blobs").iterdir():
        blob.unlink()
    assert main(["runs", "list"]) == 0
    out, err = capfd.readouterr()
    assert out.splitlines() == [
        f"handbuilt  {_local(HAND_BUILT_AT)}  -  incomplete  sdk  refund_ticket  "
        "? exchanges  ? writes"
    ]
    assert err.startswith("warning: run handbuilt could not be read: blob ")


def _ended_run(run_id: str, started_at: float, **changes) -> RunRecord:
    record = RunRecord(
        schema_version=trace.SCHEMA_VERSION,
        run_id=run_id,
        mode="shadow",
        attribution="process",
        trigger=None,
        agent_version=None,
        engine_version="0.0.0",
        sdk_version=None,
        started_at=started_at,
        ended_at=started_at + 1.0,
        outcome="ok",
        error=None,
        exit_code=0,
    )
    return dataclasses.replace(record, **changes)


def test_runs_list_shows_a_labelled_run_newer_than_the_limit_s_worth_of_process_runs(home, capfd):
    """A `header` run once had no start, so it sorted after every run that had one and the
    default `runs list` hid it behind twenty older process runs (#72)."""
    s = DirectoryStore(home / "store", redact.load_key(home))
    for i in range(DEFAULT_RUNS_LIMIT + 1):
        s.start_run(_ended_run(f"proc{i:02d}", HAND_BUILT_AT + i))
    labelled_at = HAND_BUILT_AT + 100
    s.record(dataclasses.replace(_refund("labelled"), started_at=labelled_at))
    s.close()
    assert main(["runs", "list"]) == 0
    lines = capfd.readouterr().out.splitlines()
    assert len(lines) == DEFAULT_RUNS_LIMIT
    assert lines[0] == (
        f"labelled  {_local(labelled_at)}  -  incomplete  header  -  1 exchanges  1 writes"
    )


def test_runs_list_reads_no_body_to_count_a_run_s_events(home, capfd, monkeypatch):
    """Counting needs each event's kind and answer, never its body: a few runs of LLM streams were
    hundreds of MB read to print two numbers (#72)."""
    _hand_built_incomplete_run(home / "store", redact.load_key(home))
    real_read_bytes = Path.read_bytes

    def no_blob_reads(path):
        assert "blobs" not in path.parts, f"runs list read a blob: {path}"
        return real_read_bytes(path)

    monkeypatch.setattr(Path, "read_bytes", no_blob_reads)
    assert main(["runs", "list"]) == 0
    assert capfd.readouterr().out.endswith("2 exchanges  1 writes\n")


def test_runs_list_never_prints_a_negative_duration(home, capfd):
    """The wall clock can step back between a run's start and its end (#72). Nor does the summary
    `runs show` ends in, which times the run the same way."""
    s = DirectoryStore(home / "store", redact.load_key(home))
    s.start_run(_ended_run("stepped", HAND_BUILT_AT, ended_at=HAND_BUILT_AT - 10))
    s.close()
    assert main(["runs", "list"]) == 0
    assert capfd.readouterr().out.split("  ")[2] == "0.0s"
    assert main(["runs", "show", "stepped"]) == 0
    assert "irimi shadow · run stepped · 0 exchanges · 0.0s · backstop: none (Phase 4)" in (
        capfd.readouterr().out.splitlines()
    )


# ------------------------------------------------------------------------------ runs show

HANDBUILT_EVENTS = [
    "fake-L0   write     POST api.stripe.com/v1/refunds -> 200",
    "shadow    write     tool db.mark_refunded -> raised db.Locked",
    "telemetry api.smith.langchain.com",
]


def _handbuilt_summary(write: str) -> list[str]:
    return [
        "irimi shadow · run handbuilt · 2 exchanges · 0.0s · backstop: none (Phase 4)",
        "",
        "  api.stripe.com  1 write intercepted",
        "  telemetry       1 exchange to 1 host, forwarded live",
        "",
        f"  ○ {write}  unvalidated (L0)",
        "",
        "  2 exchanges · 1 live · 0 delegated · 1 virtualized",
        "  These writes did not happen.",
    ]


def test_runs_show_prints_the_header_every_event_and_the_summary(home, capfd):
    _hand_built_incomplete_run(home / "store", redact.load_key(home))
    assert main(["runs", "show", "handbuilt"]) == 0
    out, err = capfd.readouterr()
    assert err == ""
    assert out.splitlines() == [
        "run: handbuilt",
        "attribution: sdk",
        "trigger: refund_ticket",
        "entrypoint: agent:refund_ticket",
        'args: {"ticket":"T-1"}',
        "agent version: 2.0.1",
        "outcome: incomplete",
        "",
        *HANDBUILT_EVENTS,
        "",
        # The shipped Stripe map's `human:` template, with the request's own fields in it.
        *_handbuilt_summary("refund $49.00 on ch_1"),
    ]


def test_runs_show_prints_an_error_run_s_error_and_exit_code(home, capfd):
    run_id = _shadow(capfd, "import sys", "sys.exit(3)")
    assert main(["runs", "show", run_id]) == 0
    lines = capfd.readouterr().out.splitlines()
    assert lines[:9] == [
        f"run: {run_id}",
        "attribution: process",
        f"trigger: {sys.executable}",
        "entrypoint: -",
        f'args: {{"argv":["{sys.executable}","-c","import sys\\nsys.exit(3)"]}}',
        "agent version: -",
        "outcome: error",
        "error: exit: exited 3",
        "exit code: 3",
    ]
    assert lines[9] == ""
    assert lines[10].startswith(f"irimi shadow · run {run_id} · 0 exchanges · ")


def test_runs_show_cuts_long_args_and_says_how_many_events_were_dropped(home, capfd):
    s = DirectoryStore(home / "store", redact.load_key(home))
    record = RunRecord(
        schema_version=trace.SCHEMA_VERSION,
        run_id="long",
        mode="shadow",
        attribution="sdk",
        trigger=Trigger("t", None, {"text": "x" * 600}, False),
        agent_version=None,
        engine_version="0.0.0",
        sdk_version=None,
        started_at=HAND_BUILT_AT,
        ended_at=HAND_BUILT_AT + 1.25,
        outcome="ok",
        error=None,
        exit_code=None,
        dropped_events=4,
    )
    s.start_run(record)
    s.close()
    # The store counts its own drops; this run's record says four, as a store that dropped them
    # would have written it.
    (home / "store" / "runs" / "long" / "run.json").write_text(
        json.dumps(trace.run_to_json(record))
    )
    assert main(["runs", "show", "long"]) == 0
    lines = capfd.readouterr().out.splitlines()
    assert lines[4] == "args: " + '{"text":"' + "x" * 491 + "…"
    assert len(lines[4]) == len("args: ") + 501
    assert lines[6:9] == ["outcome: ok", "dropped events: 4", ""]
    assert lines[9].startswith("irimi shadow · run long · 0 exchanges · 1.2s · ")


def test_runs_show_an_unknown_id_fails_with_the_documented_message(home, capfd):
    assert main(["runs", "show", "nope"]) == 1
    out, err = capfd.readouterr()
    assert out == ""
    assert err == f"error: no run nope in {home / 'store'}\n"


def test_runs_show_unattributed_reads_the_events_no_run_claimed(home, capfd):
    assert main(["runs", "show", "unattributed"]) == 0
    lines = capfd.readouterr().out.splitlines()
    assert lines[:2] == ["run: unattributed", "attribution: header"]
    assert lines[7:9] == [
        "",
        "irimi shadow · run unattributed · 0 exchanges · 0.0s · backstop: none (Phase 4)",
    ]


def test_runs_show_still_prints_with_a_warning_when_the_maps_do_not_load(home, capfd):
    """A broken overrides file costs `runs show` the `human:` sentences and nothing else: the
    write prints as the request it was, and the command succeeds (#72)."""
    _hand_built_incomplete_run(home / "store", redact.load_key(home))
    (home / "maps.yaml").write_text("version: 99\n")
    assert main(["runs", "show", "handbuilt"]) == 0
    out, err = capfd.readouterr()
    assert err == "warning: maps did not load; writes are shown as requests\n"
    assert out.splitlines()[-9:] == _handbuilt_summary("POST api.stripe.com/v1/refunds")


def test_tool_call_line_is_in_the_exchange_line_s_columns():
    call = ToolCall("t", "r", "db.mark_refunded", "write", "shadow", {}, None, None, 1.0, 2.0)
    assert report.tool_call_line(call) == "shadow    write     tool db.mark_refunded -> ok"
    real = ToolCall("t", "r", "crm.lookup", "read", "real", {}, {}, None, 1.0, 2.0)
    assert report.tool_call_line(real) == "real      read      tool crm.lookup -> ok"


def test_runs_show_prints_a_stored_message_s_control_characters_escaped(home, capfd):
    """A stored error is the agent's own `str(exc)`. Printed raw, a newline broke the header's one
    `key: value` per line and an escape sequence was obeyed by the terminal (#72). Every header
    field the agent wrote is escaped, a C1 control (`\\x9b`, an 8-bit CSI) as well, which JSON
    leaves raw in `args`."""
    s = DirectoryStore(home / "store", redact.load_key(home))
    s.start_run(
        _ended_run(
            "raised",
            HAND_BUILT_AT,
            trigger=Trigger("job\x1b[2J", "jobs:run\r", {"note": "\x9b2J"}, True),
            agent_version="1.0\x07",
            outcome="error",
            error=ErrorInfo("ValueError", "line one\nline two \x1b[31mred\x1b[0m é"),
            exit_code=1,
        )
    )
    s.close()
    assert main(["runs", "show", "raised"]) == 0
    lines = capfd.readouterr().out.splitlines()
    assert lines[2] == "trigger: job\\x1b[2J"
    assert lines[3:6] == [
        "entrypoint: jobs:run\\r",
        'args: {"note":"\\x9b2J"}',
        "agent version: 1.0\\x07",
    ]
    assert lines[7] == "error: ValueError: line one\\nline two \\x1b[31mred\\x1b[0m é"
    assert lines[8] == "exit code: 1"
    assert main(["runs", "list"]) == 0
    assert "  job\\x1b[2J  " in capfd.readouterr().out
