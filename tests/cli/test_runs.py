"""Reading a stored run back: its summary, `irimi runs list` and `irimi runs show`.

The claim under test is that the store holds everything the live summary prints. The first test
proves it the only way that counts: the refund agent's scenario, plus one telemetry call,
runs under the real `irimi shadow`, and the summary printed from what it stored is the block the
CLI printed on exit, line for line, but for the elapsed seconds. The rest pin the two commands'
exact output over stores built by `irimi shadow` and by hand.
"""

import dataclasses
import json
import os
import re
import shlex
import sys
import time
from pathlib import Path

import pytest

from irimi import servicemaps
from irimi.cli import main
from irimi.cli.runs import DEFAULT_RUNS_LIMIT
from irimi.core.exchange import Exchange, Precondition, Request, Response
from irimi.protocol import trace
from irimi.protocol.trace import ErrorInfo, RunRecord, ToolCall, Trigger
from irimi.record import redact
from irimi.record.store.lock import ServeLock
from irimi.record.store.reader import StoreReader
from irimi.record.store.writer import DirectoryStore
from irimi.report.model import RunView
from irimi.report.runs import (
    ARGS_TOO_DEEP,
    MAX_TRIGGER_SHOWN,
    RUN_LIST_HEADER,
    run_attribution,
    run_header_lines,
    run_list_columns,
)
from irimi.report.summary import NO_TRAFFIC, Live, summary_lines, tool_write_line
from irimi.report.terminal import printable, tool_call_line
from tests.support import builders
from tests.support.maps import PRECONDITION_STRIPE_MAP
from tests.support.scenarios import REFUND_SUMMARY, run_refund_scenario

# The loopback Stripe's map with one telemetry route on it. A GET, because the stand-in fails the
# helper on any other verb it hears; telemetry is forwarded live, so the stand-in answers it.
TELEMETRY_ROUTE = """  - match:
      method: GET
      path: /v1/telemetry
    operation: telemetry.send
    kind: telemetry
"""
STRIPE_WITH_TELEMETRY = PRECONDITION_STRIPE_MAP.replace("routes:\n", "routes:\n" + TELEMETRY_ROUTE)
ELAPSED = re.compile(r" · \d+\.\ds$")


def _summary_block(lines: list[str]) -> list[str]:
    """The live summary, from its `N exchanges` header to the end of what the CLI printed."""
    header = next(i for i, line in enumerate(lines) if re.search(r" · \d+ exchanges? · ", line))
    return lines[header:]


def _no_elapsed(lines: list[str]) -> list[str]:
    return [ELAPSED.sub(" · <elapsed>", line) for line in lines]


def _process_run(store) -> RunRecord:
    (run,) = [r for r in StoreReader(store).list_runs() if r.attribution == "process"]
    return run


def test_the_refund_scenario_prints_the_same_summary_stored_as_live(
    home, tmp_path, capfd, monkeypatch
):
    """A stored run prints the summary it printed live. The only field allowed to differ is the
    elapsed seconds: the live summary times the child, and the stored one the run record's own
    start and end."""
    run_refund_scenario(
        tmp_path,
        monkeypatch,
        stripe_map=STRIPE_WITH_TELEMETRY,
        calls='call("GET", "/v1/telemetry")\n',
    )
    live = _summary_block(capfd.readouterr().out.splitlines())
    # The refund agent's block, with the telemetry call on a line of its own and in the counts.
    assert live[1:] == [
        *REFUND_SUMMARY[:-2],
        "  telemetry  1 exchange to 1 host, forwarded live",
        "",
        "  8 exchanges · 6 real · 2 answered by irimi (2 faked writes) · 0 sent to targets",
    ]

    store = home / "store"
    run = StoreReader(store).load_run(_process_run(store).run_id)
    assert [type(e).__name__ for e in run.events].count("TelemetrySeen") == 1
    stored = summary_lines(RunView(run.record, run.events), servicemaps.load())
    assert _no_elapsed(stored) == _no_elapsed(live)

    # And `runs show` ends in that same block, maps loaded as `shadow` loaded them.
    assert main(["runs", "show", run.record.run_id]) == 0
    shown = capfd.readouterr().out.splitlines()
    assert _no_elapsed(shown[-len(live) :]) == _no_elapsed(live)


# ------------------------------------------------------------------------------ runs list


def _rows(out: str) -> list[list[str]]:
    """`irimi runs list`'s rows, each split into its cells: the header row comes first, and the
    columns are padded, so two spaces or more separate one cell from the next."""
    header, *rows = out.splitlines()
    assert header.split() == list(RUN_LIST_HEADER), header
    return [re.split(r"  +", row.rstrip()) for row in rows]


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
    """A refund irimi faked at L0 without a precondition check, in a hand-built run."""
    return builders.refund_exchange(
        answered_by="fake-L0",
        precondition=Precondition(),
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
    *shadowed, handbuilt = _rows(capfd.readouterr().out)
    # A process run's trigger is its command line, the program without its directory.
    python = os.path.basename(sys.executable)
    failing = printable(shlex.join([python, "-c", "import sys\nsys.exit(3)"]))
    assert [[re.sub(STARTED, "<started>", c) for c in row] for row in shadowed] == [
        [error, "<started>", shadowed[0][2], "error (exit 3)", "process", failing]
        + ["0 exchanges", "0 writes"],
        [ok, "<started>", shadowed[1][2], "ok", "process", f"{python} -c pass"]
        + ["0 exchanges", "0 writes"],
    ]
    assert all(re.fullmatch(r"\d+\.\ds", row[2]) for row in shadowed)
    assert handbuilt == [
        "handbuilt",
        _local(HAND_BUILT_AT),
        "-",
        "incomplete",
        "sdk",
        "refund_ticket",
        "2 exchanges",
        "1 write",
    ]

    assert main(["runs", "list", "--limit", "1"]) == 0
    out = capfd.readouterr().out
    assert [row[0] for row in _rows(out.split("\n(")[0] + "\n")] == [error]
    assert out.splitlines()[-1] == "(2 older runs; use --limit or --all)"


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
    assert _rows(out) == [
        ["handbuilt", _local(HAND_BUILT_AT), "-", "incomplete", "sdk", "refund_ticket"]
        + ["? exchanges", "? writes"]
    ]
    events = home / "store" / "runs" / "handbuilt" / "events.jsonl"
    assert err.startswith(f"warning: run handbuilt could not be read: {events}, line 1: blob ")


def test_runs_list_counts_the_writes_runs_show_s_summary_prints_a_line_for(home, capfd):
    """A refund the precondition check rejected never entered the write log, but the summary
    prints it (`would fail`), and so `runs list` counts it."""
    rejected = dataclasses.replace(
        _refund("checked"),
        answered_by="fake-L1",
        precondition=Precondition("rejected", "charge_already_refunded"),
    )
    s = DirectoryStore(home / "store", redact.load_key(home))
    s.start_run(_ended_run("checked", HAND_BUILT_AT))
    s.record(rejected)
    s.close()
    assert main(["runs", "list"]) == 0
    listed = capfd.readouterr().out
    assert listed.endswith("  1 exchange  1 write\n")
    assert main(["runs", "show", "checked"]) == 0
    shown = capfd.readouterr().out.splitlines()
    assert "  api.stripe.com  1 faked write" in shown
    assert "  ✗ stripe  refund $49.00 on ch_1  faked; would fail: charge_already_refunded" in shown


def _ended_run(run_id: str, started_at: float, **changes) -> RunRecord:
    end = trace.RunEnd(
        changes.pop("ended_at", started_at + 1.0),
        changes.pop("outcome", "ok"),
        changes.pop("error", None),
        changes.pop("exit_code", 0),
    )
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
        end=end,
    )
    return dataclasses.replace(record, **changes)


def test_runs_list_shows_a_labelled_run_newer_than_the_limit_s_worth_of_process_runs(home, capfd):
    """A `header` run once had no start, so it sorted after every run that had one and the
    default `runs list` hid it behind twenty older process runs."""
    s = DirectoryStore(home / "store", redact.load_key(home))
    for i in range(DEFAULT_RUNS_LIMIT + 1):
        s.start_run(_ended_run(f"proc{i:02d}", HAND_BUILT_AT + i))
    labelled_at = HAND_BUILT_AT + 100
    s.record(dataclasses.replace(_refund("labelled"), started_at=labelled_at))
    s.close()
    assert main(["runs", "list"]) == 0
    *listed, older = capfd.readouterr().out.splitlines()
    rows = _rows("\n".join(listed))
    assert len(rows) == DEFAULT_RUNS_LIMIT
    assert rows[0] == [
        "labelled",
        _local(labelled_at),
        "-",
        "incomplete",
        "header",
        "-",
        "1 exchange",
        "1 write",
    ]
    assert older == "(2 older runs; use --limit or --all)"


def test_runs_list_and_show_name_the_run_a_replay_replayed(home, capfd):
    """`irimi replay` stores its one run `attribution: process`, as `irimi shadow` stores one, so
    `runs list` lists a replay as the run it replayed, `replay of <id>`, and `runs show` prints
    its mode and the run it replayed under the attribution, and opens its summary `irimi replay`.
    Every run a replay stores names `replay_of` and `mode: replay`: an `sdk` run started there
    and a `header` run an `Irimi-Run` label makes there too, and each keeps its attribution in
    the column."""
    s = DirectoryStore(home / "store", redact.load_key(home))
    s.start_run(_ended_run("recorded", HAND_BUILT_AT))
    s.close()
    replays = DirectoryStore(home / "store", redact.load_key(home), replay_of="recorded")
    replay = {"mode": "replay", "replay_of": "recorded"}
    replays.start_run(_ended_run("replayed", HAND_BUILT_AT + 5, **replay))
    replays.start_run(_ended_run("started", HAND_BUILT_AT + 6, attribution="sdk", **replay))
    replays.record(dataclasses.replace(_refund("labelled"), started_at=HAND_BUILT_AT + 7))
    replays.close()
    assert main(["runs", "list"]) == 0
    assert _rows(capfd.readouterr().out) == [
        ["labelled", _local(HAND_BUILT_AT + 7), "-", "incomplete", "header, replay of recorded"]
        + ["-", "1 exchange", "1 write"],
        ["started", _local(HAND_BUILT_AT + 6), "1.0s", "ok", "sdk, replay of recorded"]
        + ["-", "0 exchanges", "0 writes"],
        ["replayed", _local(HAND_BUILT_AT + 5), "1.0s", "ok", "replay of recorded"]
        + ["-", "0 exchanges", "0 writes"],
        ["recorded", _local(HAND_BUILT_AT), "1.0s", "ok", "process", "-"]
        + ["0 exchanges", "0 writes"],
    ]
    assert main(["runs", "show", "replayed"]) == 0
    shown = capfd.readouterr().out.splitlines()
    assert shown[:5] == [
        "run: replayed",
        "attribution: process",
        "mode: replay",
        "replay of: recorded",
        "trigger: -",
    ]
    assert "irimi replay · run replayed · 0 exchanges · 1.0s" in shown
    assert not any(line.startswith("irimi shadow · ") for line in shown)
    assert main(["runs", "show", "recorded"]) == 0
    shown = capfd.readouterr().out.splitlines()
    assert shown[:3] == [
        "run: recorded",
        "attribution: process",
        "trigger: -",
    ]
    assert "irimi shadow · run recorded · 0 exchanges · 1.0s" in shown


def test_a_damaged_replay_record_is_still_marked_and_prints_on_one_line():
    """No irimi writes a `mode: replay` run that names no run, or a `replay_of` that is not a run
    id, and the decoder refuses a run.json that holds either (the next test), but
    `report` renders the record it is handed: the first is still marked a replay, and the second
    is escaped as every stored text is."""
    unnamed = _ended_run("x", HAND_BUILT_AT, mode="replay")
    assert "mode: replay" in run_header_lines(unnamed)
    odd = _ended_run("y", HAND_BUILT_AT, attribution="sdk", replay_of="a\nb\x1b")
    assert run_attribution(odd) == "sdk, replay of a\\nb\\x1b"
    assert "replay of: a\\nb\\x1b" in run_header_lines(odd)
    assert not any("\n" in cell for cell in run_list_columns(odd, []))
    assert run_attribution(_ended_run("z", HAND_BUILT_AT)) == "process"


def test_a_replay_s_run_json_naming_no_run_is_skipped_by_list_and_refused_by_show(home, capfd):
    """A `replay_of` with two spaces broke `runs list`'s columns, and a 5 MB one was printed whole
    on its line. The decoder refuses one `is_valid_run_id` refuses, by its length, so
    `runs list` skips the run, as it skips any run.json it cannot read, and `runs show`
    names the file and why."""
    s = DirectoryStore(home / "store", redact.load_key(home))
    s.start_run(_ended_run("recorded", HAND_BUILT_AT))
    s.close()
    replays = DirectoryStore(home / "store", redact.load_key(home), replay_of="recorded")
    replays.start_run(
        _ended_run("replayed", HAND_BUILT_AT + 5, mode="replay", replay_of="recorded")
    )
    replays.close()
    run_json = home / "store" / "runs" / "replayed" / "run.json"
    for replay_of in ("a  b", "x" * 5_000_000):
        run_json.write_text(json.dumps(json.loads(run_json.read_text()) | {"replay_of": replay_of}))
        assert main(["runs", "list"]) == 0
        assert _rows(capfd.readouterr().out) == [
            ["recorded", _local(HAND_BUILT_AT), "1.0s", "ok", "process", "-"]
            + ["0 exchanges", "0 writes"]
        ]
        assert main(["runs", "show", "replayed"]) == 2
        assert capfd.readouterr() == (
            "",
            f"error: run replayed could not be read: {run_json}: 'replay_of' is not a run id "
            f"({len(replay_of)} characters)\n",
        )


def test_runs_list_reads_no_body_to_count_a_run_s_events(home, capfd, monkeypatch):
    """Counting needs each event's kind and answer, never its body: a few runs of LLM streams were
    hundreds of MB read to print two numbers."""
    _hand_built_incomplete_run(home / "store", redact.load_key(home))
    real_read_bytes = Path.read_bytes

    def no_blob_reads(path):
        assert "blobs" not in path.parts, f"runs list read a blob: {path}"
        return real_read_bytes(path)

    monkeypatch.setattr(Path, "read_bytes", no_blob_reads)
    assert main(["runs", "list"]) == 0
    assert capfd.readouterr().out.endswith("2 exchanges  1 write\n")


def test_runs_list_never_prints_a_negative_duration(home, capfd):
    """The wall clock can step back between a run's start and its end. Nor does the summary
    `runs show` ends in, which times the run the same way."""
    s = DirectoryStore(home / "store", redact.load_key(home))
    s.start_run(_ended_run("stepped", HAND_BUILT_AT, ended_at=HAND_BUILT_AT - 10))
    s.close()
    assert main(["runs", "list"]) == 0
    assert _rows(capfd.readouterr().out)[0][2] == "0.0s"
    assert main(["runs", "show", "stepped"]) == 0
    assert "irimi shadow · run stepped · 0 exchanges · 0.0s" in (
        capfd.readouterr().out.splitlines()
    )


def test_runs_list_pads_each_column_to_its_widest_cell_under_a_header_row(home, capfd):
    s = DirectoryStore(home / "store", redact.load_key(home))
    s.start_run(_ended_run("short", HAND_BUILT_AT))
    s.start_run(_ended_run("a-much-longer-id", HAND_BUILT_AT + 1, outcome="error", exit_code=12))
    s.close()
    assert main(["runs", "list"]) == 0
    header, *rows = capfd.readouterr().out.splitlines()
    assert header.split() == list(RUN_LIST_HEADER)
    starts = [header.index(name) for name in RUN_LIST_HEADER]
    for row in rows:
        for start in starts[1:]:
            assert row[start - 2 : start] == "  " and row[start] != " ", (row, start)
    assert [cells[3] for cells in _rows("\n".join([header, *rows]))] == ["error (exit 12)", "ok"]


def test_runs_list_shows_a_process_run_s_command_without_its_program_s_directory():
    """Every run of `python agent.py` names the same interpreter path, so the trigger column is
    the program's base name and its arguments, cut to forty characters."""
    argv = ["/opt/venv/bin/python3", "agent.py", "--ticket", "T-1"]
    run = _ended_run("p", HAND_BUILT_AT, trigger=Trigger.command(argv))
    assert run_list_columns(run, [])[5] == "python3 agent.py --ticket T-1"
    long = ["/usr/bin/python3", "agent.py", "x" * 60]
    run = _ended_run("q", HAND_BUILT_AT, trigger=Trigger.command(long))
    cell = run_list_columns(run, [])[5]
    assert len(cell) == MAX_TRIGGER_SHOWN
    assert cell == ("python3 agent.py " + "x" * 60)[: MAX_TRIGGER_SHOWN - 1] + "…"


def test_runs_list_says_how_many_older_runs_it_left_out_and_all_lists_every_one(home, capfd):
    s = DirectoryStore(home / "store", redact.load_key(home))
    for i in range(DEFAULT_RUNS_LIMIT + 2):
        s.start_run(_ended_run(f"proc{i:02d}", HAND_BUILT_AT + i))
    s.close()
    assert main(["runs", "list", "--limit", "3"]) == 0
    *listed, older = capfd.readouterr().out.splitlines()
    assert [row[0] for row in _rows("\n".join(listed))] == ["proc21", "proc20", "proc19"]
    assert older == f"({DEFAULT_RUNS_LIMIT - 1} older runs; use --limit or --all)"
    assert main(["runs", "list", "--all"]) == 0
    rows = _rows(capfd.readouterr().out)
    assert len(rows) == DEFAULT_RUNS_LIMIT + 2


# ------------------------------------------------------------------------------ runs show


def test_runs_show_takes_a_prefix_only_one_run_id_starts_with(home, capfd):
    s = DirectoryStore(home / "store", redact.load_key(home))
    s.start_run(_ended_run("9c207d4b1c0178f3", HAND_BUILT_AT))
    s.start_run(_ended_run("9c3aa0000000beef", HAND_BUILT_AT + 1))
    s.close()
    assert main(["runs", "show", "9c20"]) == 0
    assert capfd.readouterr().out.splitlines()[0] == "run: 9c207d4b1c0178f3"
    assert main(["runs", "show", "9c"]) == 2
    assert capfd.readouterr() == (
        "",
        "error: run id 9c is ambiguous; it starts 9c207d4b1c0178f3, 9c3aa0000000beef\n",
    )


def test_runs_show_with_no_run_id_shows_the_newest_run(home, capfd):
    s = DirectoryStore(home / "store", redact.load_key(home))
    s.start_run(_ended_run("older", HAND_BUILT_AT))
    s.start_run(_ended_run("newer", HAND_BUILT_AT + 1))
    s.close()
    assert main(["runs", "show"]) == 0
    assert capfd.readouterr().out.splitlines()[0] == "run: newer"
    assert main(["runs", "show", "last"]) == 0
    assert capfd.readouterr().out.splitlines()[0] == "run: newer"


def test_runs_show_last_in_an_empty_store_says_there_are_no_runs(home, capfd):
    assert main(["runs", "show"]) == 2
    assert capfd.readouterr() == ("", f"error: no runs in {home / 'store'}\n")


HANDBUILT_EVENTS = [
    "faked         write     POST api.stripe.com/v1/refunds -> 200",
    "tool-fake     write     db.mark_refunded -> raised db.Locked",
    "telemetry api.smith.langchain.com",
]


def _handbuilt_summary(write: str, money: list[str]) -> list[str]:
    return [
        "irimi shadow · run handbuilt · 2 exchanges · 0.0s",
        "",
        "  None of these writes reached the real service:",
        f"  ○ stripe  {write}  faked; not checked",
        "  ○ tool  db.mark_refunded  fake raised db.Locked",
        *money,
        "",
        "  api.stripe.com  1 faked write",
        "  telemetry       1 exchange to 1 host, forwarded live",
        "",
        "  2 exchanges · 1 real · 1 answered by irimi (1 faked write) · 0 sent to targets",
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
        *_handbuilt_summary("refund $49.00 on ch_1", ["  money: $49.00 in faked writes"]),
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
        end=trace.RunEnd(HAND_BUILT_AT + 1.25, "ok", None, None),
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
    assert lines[9] == "irimi shadow · run long · 0 exchanges · 1.2s"


def test_runs_show_an_unknown_id_fails_with_the_documented_message(home, capfd):
    assert main(["runs", "show", "nope"]) == 2
    out, err = capfd.readouterr()
    assert out == ""
    assert err == f"error: no run nope in {home / 'store'}\n"


def test_runs_show_unattributed_on_an_empty_pool_says_so_and_invents_no_run(home, capfd):
    assert main(["runs", "show", "unattributed"]) == 0
    out, err = capfd.readouterr()
    assert out == f"no unattributed events in {home / 'store'}\n"
    assert err == ""


def test_runs_show_unattributed_reads_the_events_no_run_claimed(home, capfd):
    s = DirectoryStore(home / "store", redact.load_key(home))
    s.record(_refund("unattributed"))
    s.close()
    assert main(["runs", "show", "unattributed"]) == 0
    lines = capfd.readouterr().out.splitlines()
    assert lines[:2] == ["run: unattributed", "attribution: header"]
    assert "irimi shadow · run unattributed · 1 exchange · 0.0s" in lines


def test_runs_show_still_prints_with_a_warning_when_the_maps_do_not_load(home, capfd):
    """A broken overrides file costs `runs show` the `human:` sentences and nothing else: the
    write prints as the request it was, and the command succeeds."""
    _hand_built_incomplete_run(home / "store", redact.load_key(home))
    (home / "maps.yaml").write_text("version: 99\n")
    assert main(["runs", "show", "handbuilt"]) == 0
    out, err = capfd.readouterr()
    assert err == "warning: maps did not load; writes are shown as requests\n"
    # No `human:` sentence names the amount, so there is no money line either.
    summary = _handbuilt_summary("POST api.stripe.com/v1/refunds", [])
    assert out.splitlines()[-len(summary) :] == summary


def test_a_tool_call_line_escapes_the_control_characters_it_was_posted_with():
    """A name and an error type are whatever reached the control endpoint: one call is one
    line, and an escape sequence is shown, never obeyed."""
    name = "evil\x1b[2J\nshadow    write     tool x"
    call = ToolCall("t", "r", name, "write", "shadow", {}, None, ErrorInfo("a\nb", "m"), 1.0, 2.0)
    escaped = "evil\\x1b[2J\\nshadow    write     tool x"
    assert tool_call_line(call) == f"tool-fake     write     {escaped} -> raised a\\nb"
    assert tool_write_line(call) == f"  ○ tool  {escaped}  fake raised a\\nb"


def test_a_run_whose_only_writes_are_tool_writes_still_says_they_did_not_happen():
    """A write tool whose stand-in ran did not happen, as a faked HTTP write did not; a read
    tool, which ran for real, gets no line and no closing sentence."""
    write = ToolCall("t", "r", "db.write", "write", "shadow", {}, 1, None, 1.0, 2.0)
    read = ToolCall("u", "r", "db.read", "read", "real", {}, 1, None, 1.0, 2.0)
    totals = "  0 exchanges · 0 real · 0 answered by irimi (0 faked writes) · 0 sent to targets"
    view = RunView(RunRecord.header("r"), [read, write])
    assert summary_lines(view, None, live=Live()) == [
        "irimi shadow · run r · 0 exchanges · 0.0s",
        "",
        "  None of these writes reached the real service:",
        "  ○ tool  db.write  fake ran instead",
        "",
        totals,
        f"  {NO_TRAFFIC}",
    ]
    view = RunView(RunRecord.header("r"), [read])
    assert summary_lines(view, None, live=Live()) == [
        "irimi shadow · run r · 0 exchanges · 0.0s",
        "",
        totals,
        f"  {NO_TRAFFIC}",
    ]


def test_tool_call_line_is_in_the_exchange_line_s_columns():
    call = ToolCall("t", "r", "db.mark_refunded", "write", "shadow", {}, None, None, 1.0, 2.0)
    assert tool_call_line(call) == "tool-fake     write     db.mark_refunded -> ok"
    real = ToolCall("t", "r", "crm.lookup", "read", "real", {}, {}, None, 1.0, 2.0)
    assert tool_call_line(real) == "tool-real     read      crm.lookup -> ok"


def test_runs_show_prints_a_stored_message_s_control_characters_escaped(home, capfd):
    """A stored error is the agent's own `str(exc)`. Printed raw, a newline broke the header's one
    `key: value` per line and an escape sequence was obeyed by the terminal. Every header
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


def test_runs_show_prints_a_lone_surrogate_escaped_and_never_raises(home, capfd):
    """A lone surrogate is what `os.fsdecode` makes of a filename that is not UTF-8, and JSON
    carries it as `\\udcff`. Printed raw, it raised UnicodeEncodeError out of `irimi runs show`
    and `runs list`: a stored run must print, whatever the agent stored."""
    s = DirectoryStore(home / "store", redact.load_key(home))
    s.start_run(
        _ended_run(
            "surrogate",
            HAND_BUILT_AT,
            trigger=Trigger("job\udcff", "jobs:run", {"path": "/tmp/\udcff.csv"}, True),
            outcome="error",
            error=ErrorInfo("OSError", "no such file: /tmp/\udcff.csv"),
            exit_code=1,
        )
    )
    s.close()
    assert main(["runs", "show", "surrogate"]) == 0
    lines = capfd.readouterr().out.splitlines()
    assert lines[2] == "trigger: job\\udcff"
    assert lines[4] == 'args: {"path":"/tmp/\\udcff.csv"}'
    assert lines[7] == "error: OSError: no such file: /tmp/\\udcff.csv"
    assert main(["runs", "list"]) == 0
    assert "  job\\udcff  " in capfd.readouterr().out


# ------------------------------------------------------------------- a value nested too deep
#
# A line nested this deep is read by every Python's `json.loads` (3.12's stops near 10,000), so its
# refusal is the decoder's everywhere. 3.14's reads one 100,000 deep too, which 3.12's refuses as
# the parse's damage, and there a decoder that quoted the value with `repr` raised RecursionError
# out of `irimi runs show` as a traceback: `StoreReader` had no guard of its own.
_READ_EVERYWHERE = 5_000
_READ_ON_3_14 = 100_000


@pytest.mark.parametrize("depth", [_READ_EVERYWHERE, _READ_ON_3_14])
def test_runs_show_and_list_name_an_event_nested_too_deep_to_quote_as_damage(home, capfd, depth):
    """Damage like any other: `runs show` names it and exits 1, and `runs list` still lists the
    run, its counts `?`. Never the value itself, and never a traceback."""
    _hand_built_incomplete_run(home / "store", redact.load_key(home))
    events = home / "store" / "runs" / "handbuilt" / "events.jsonl"
    stored = events.read_text()
    deep = "[" * depth + "]" * depth
    events.write_text(stored.replace('"name": "db.mark_refunded"', f'"name": {deep}'))
    assert events.read_text() != stored
    assert main(["runs", "show", "handbuilt"]) == 2
    out, err = capfd.readouterr()
    refused = "error: run handbuilt could not be read: "
    assert out == "" and err.startswith(refused) and err.count("\n") == 1 and "[[" not in err
    if depth == _READ_EVERYWHERE:
        assert err == f"{refused}{events}, line 2: 'name' is a list, not a string\n"
    assert main(["runs", "list"]) == 0
    out, err = capfd.readouterr()
    assert _rows(out) == [
        ["handbuilt", _local(HAND_BUILT_AT), "-", "incomplete", "sdk", "refund_ticket"]
        + ["? exchanges", "? writes"]
    ]
    assert err.startswith("warning: run handbuilt could not be read: ") and "[[" not in err


def test_runs_show_names_a_run_json_nested_too_deep_to_quote_as_damage(home, capfd):
    """A run.json is read by the same decoders, and `list_runs` skips one it cannot read."""
    _hand_built_incomplete_run(home / "store", redact.load_key(home))
    run_json = home / "store" / "runs" / "handbuilt" / "run.json"
    deep = "[" * _READ_EVERYWHERE + "]" * _READ_EVERYWHERE
    stored = run_json.read_text()
    run_json.write_text(stored.replace('"sdk_version": "0.0.0"', f'"sdk_version": {deep}'))
    assert run_json.read_text() != stored
    assert main(["runs", "show", "handbuilt"]) == 2
    assert capfd.readouterr() == (
        "",
        f"error: run handbuilt could not be read: {run_json}: 'sdk_version' is a list, not a "
        "string\n",
    )
    assert main(["runs", "list"]) == 0
    assert capfd.readouterr().out == f"no runs in {home / 'store'}\n"


def test_runs_show_says_a_trigger_s_args_nest_too_deeply_to_show_and_never_raises():
    """Python 3.14's `json.loads` reads a run.json some levels deeper than its `json.dumps` writes
    back, so a damaged one read fine and then raised RecursionError out of `runs show`.
    Built in Python a million deep, past what either Python writes, so the same on both."""
    args: list = []
    for _ in range(1_000_000):
        args = [args]
    record = _ended_run("deep", HAND_BUILT_AT, trigger=Trigger("job", None, args, False))
    assert run_header_lines(record)[4] == f"args: {ARGS_TOO_DEEP}"


# ---------------------------------------------------------------------- runs rm and runs prune


def _store_with(home, *exchanges: Exchange, runs: tuple[RunRecord, ...] = ()) -> Path:
    s = DirectoryStore(home / "store", redact.load_key(home))
    for record in runs:
        s.start_run(record)
    for exchange in exchanges:
        s.record(exchange)
    s.close()
    return home / "store"


def _own_body(run_id: str) -> Exchange:
    """A refund whose request body no other run's is, so its blob is this run's alone."""
    refund = _refund(run_id)
    body = f"charge=ch_{run_id}&amount=4900&currency=usd".encode()
    return dataclasses.replace(refund, request=dataclasses.replace(refund.request, body=body))


def _blobs(store: Path) -> set[str]:
    return {blob.name for blob in (store / "blobs").iterdir()}


def test_runs_rm_deletes_a_run_and_only_the_bodies_no_other_run_uses(home, capfd):
    store = _store_with(home, _own_body("kept"), _own_body("gone"))
    before = _blobs(store)
    gone = trace.body_ref(_own_body("gone").request.body).sha256
    assert main(["runs", "rm", "gone"]) == 0
    assert capfd.readouterr() == (
        "deleted run gone\ndeleted 1 run and 1 stored body no other run uses\n",
        "",
    )
    assert not (store / "runs" / "gone").exists()
    # The response body `{}` both runs stored stays: the run kept still reads it whole.
    assert _blobs(store) == before - {gone}
    assert StoreReader(store).load_run("kept").events


def test_runs_rm_takes_several_runs_a_prefix_and_last(home, capfd):
    store = _store_with(
        home,
        runs=(
            _ended_run("9c207d4b1c0178f3", HAND_BUILT_AT),
            _ended_run("aa00000000000001", HAND_BUILT_AT + 1),
            _ended_run("bb00000000000002", HAND_BUILT_AT + 2),
        ),
    )
    assert main(["runs", "rm", "9c20", "last"]) == 0
    assert capfd.readouterr().out.splitlines() == [
        "deleted run 9c207d4b1c0178f3",
        "deleted run bb00000000000002",
        "deleted 2 runs and 0 stored bodies no other run uses",
    ]
    assert [r.run_id for r in StoreReader(store).list_runs()] == ["aa00000000000001"]


def test_runs_rm_deletes_nothing_when_any_run_named_is_not_stored(home, capfd):
    store = _store_with(home, runs=(_ended_run("kept", HAND_BUILT_AT),))
    assert main(["runs", "rm", "kept", "nope"]) == 2
    assert capfd.readouterr() == ("", f"error: no run nope in {store}\n")
    assert [r.run_id for r in StoreReader(store).list_runs()] == ["kept"]


def test_runs_rm_refuses_an_ambiguous_prefix(home, capfd):
    _store_with(home, runs=(_ended_run("ab1", HAND_BUILT_AT), _ended_run("ab2", HAND_BUILT_AT)))
    assert main(["runs", "rm", "ab"]) == 2
    assert capfd.readouterr() == ("", "error: run id ab is ambiguous; it starts ab1, ab2\n")


def test_runs_rm_is_refused_while_a_serve_writes_the_store(home, capfd):
    store = _store_with(home, runs=(_ended_run("kept", HAND_BUILT_AT),))
    lock = ServeLock.hold(store)
    try:
        assert main(["runs", "rm", "kept"]) == 2
    finally:
        lock.release()
    assert capfd.readouterr() == (
        "",
        f"error: another irimi serve is writing the trace store {store}; stop it to delete runs\n",
    )
    assert (store / "runs" / "kept").is_dir()


def test_runs_rm_deletes_a_damaged_run_named_in_full(home, capfd):
    """`runs list` skips a run.json it cannot read, but the run is still deletable by its id."""
    store = _store_with(home, _own_body("broken"))
    (store / "runs" / "broken" / "run.json").write_text("{")
    assert main(["runs", "rm", "broken"]) == 0
    assert capfd.readouterr().out.splitlines()[0] == "deleted run broken"
    assert not (store / "runs" / "broken").exists()
    assert _blobs(store) == set()


def test_runs_rm_unattributed_deletes_the_events_no_run_claimed(home, capfd):
    store = _store_with(home, _own_body("unattributed"))
    assert main(["runs", "rm", "unattributed"]) == 0
    assert capfd.readouterr().out.splitlines()[0] == "deleted run unattributed"
    assert not (store / "unattributed").exists()
    assert _blobs(store) == set()


def test_runs_prune_deletes_the_runs_older_than_the_duration_and_dry_run_deletes_nothing(
    home, capfd
):
    now = time.time()
    store = _store_with(
        home,
        runs=(
            _ended_run("month", now - 31 * 86400),
            # Started long ago but ended inside the window: a run's age is from its end.
            _ended_run("long", now - 40 * 86400, ended_at=now - 3 * 3600),
            _ended_run("fresh", now - 60),
        ),
    )
    assert main(["runs", "prune", "--older-than", "30d", "--dry-run"]) == 0
    assert capfd.readouterr().out.splitlines() == [
        "would delete run month",
        "would delete 1 run and 0 stored bodies no other run uses",
    ]
    assert len(StoreReader(store).list_runs()) == 3
    assert main(["runs", "prune", "--older-than", "30d"]) == 0
    assert capfd.readouterr().out.splitlines()[0] == "deleted run month"
    assert main(["runs", "prune", "--older-than", "2h"]) == 0
    assert capfd.readouterr().out.splitlines()[0] == "deleted run long"
    assert main(["runs", "prune", "--older-than", "2h"]) == 0
    assert capfd.readouterr().out == f"no runs older than 2h in {store}\n"
    assert [r.run_id for r in StoreReader(store).list_runs()] == ["fresh"]


def test_runs_prune_judges_a_run_that_has_not_ended_by_when_its_files_were_last_written(
    home, capfd
):
    """A run an `irimi shadow` is still writing has no end, and may have started long ago."""
    long_ago = time.time() - 2 * 86400
    store = _store_with(home, dataclasses.replace(_own_body("stuck"), started_at=long_ago))
    assert main(["runs", "prune", "--older-than", "1d"]) == 0
    assert capfd.readouterr().out == f"no runs older than 1d in {store}\n"
    run_dir = store / "runs" / "stuck"
    os.utime(run_dir / "run.json", (long_ago, long_ago))
    assert main(["runs", "prune", "--older-than", "1d"]) == 0
    assert capfd.readouterr().out == f"no runs older than 1d in {store}\n"
    os.utime(run_dir / "events.jsonl", (long_ago, long_ago))
    assert main(["runs", "prune", "--older-than", "1d"]) == 0
    assert capfd.readouterr().out.splitlines() == [
        "deleted run stuck",
        # Its request's body and its response's `{}`.
        "deleted 1 run and 2 stored bodies no other run uses",
    ]


@pytest.mark.parametrize("older_than", ["30", "1.5d", "30m", "-1d", "d"])
def test_runs_prune_refuses_a_duration_that_is_not_whole_hours_or_days(older_than, capsys):
    with pytest.raises(SystemExit) as exited:
        main(["runs", "prune", "--older-than", older_than])
    assert exited.value.code == 2
    assert "give a whole number of hours or days, as 12h or 30d" in capsys.readouterr().err
