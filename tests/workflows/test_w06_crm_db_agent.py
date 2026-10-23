"""W6 `crm_db_agent`: labelled database and file writes run as their stand-ins under shadow.

What a tool call did is read from the agent's observation log (`tool` events say which body ran)
and from the state directory itself: the SQLite CRM and the files the write tools would create.
What irimi made of it is read from its stream, its summary and its store: `@sdk.tool` posts every
call to the run it was made in.
"""

import pytest

from examples.workflows.agentkit import LOOKUP_REFUSED_ENV
from examples.workflows.harness.run import CANARIES, check_invariants, compare, rows_not_shown
from examples.workflows.w06_crm_db_agent.agent import (
    CONNECT_RETRIES,
    DB_NAME,
    SEED,
    UNREACHABLE_HOST,
    DriverOptions,
)
from examples.workflows.w06_crm_db_agent.scenarios import STALE_REPLAY_RUN
from irimi import redact
from irimi.sdk.capture import MAX_REPR, REPR_KEY
from irimi.trace import MAX_ERROR_MESSAGE, UNATTRIBUTED, ErrorInfo, ToolCall

W = "w06_crm_db_agent"
# Each test runs under `irimi shadow -- <cmd>` and under serve mode, its agent beside irimi.
MODES = ("shadow", "serve")
WRITE_TOOLS = [
    "crm.upsert_account",
    "crm.upsert_account",
    "crm.bulk_update",
    "crm.delete_stale",
    "crm.export_csv",
    "crm.notify_owner",
]
# The totals line of a run whose only exchanges are the two enrichment reads.
TWO_REAL = "  2 exchanges · 2 real · 0 answered by irimi (0 faked writes) · 0 sent to targets"
# The seeded `smb` accounts, as `find_accounts` returns them.
SEED_SMB = [
    {
        "id": i,
        "name": n,
        "segment": s,
        "last_seen": seen,
        "score": score,
        "industry": industry,
    }
    for i, n, s, seen, score, industry in SEED
    if s == "smb"
]


# The CRM after `_touch_last_seen("acc_1")`, the write no label covers, which is real in every mode.
TOUCHED = [("acc_1", "Acme", "smb", "2026-09-28", 0, None), *SEED[1:]]


def rows(result):
    return result.query(DB_NAME, "SELECT * FROM accounts ORDER BY id")


def tool_calls(result) -> list[ToolCall]:
    """The tool calls irimi stored, in the order the run made them: one run holds them all."""
    return [e for e in result.stored_events() if isinstance(e, ToolCall)]


def files(result):
    return sorted(str(p.relative_to(result.state)) for p in result.state.rglob("*") if p.is_file())


@pytest.mark.parametrize("mode", MODES)
def test_under_shadow_every_write_tool_runs_its_stand_in_and_the_crm_is_as_seeded(
    run_workflow, mode
):
    shadow = run_workflow(W, "enrich", mode)
    assert shadow.tools("write") == [(name, "shadow") for name in WRITE_TOOLS]
    assert rows(shadow) == SEED
    assert files(shadow) == ["crm.sqlite3"]
    # The stand-ins' answers are what the agent reported, not the real bodies'.
    assert shadow.result()["deleted"] == 0
    assert shadow.result()["exported"] == "<not written: smb.csv>"
    assert shadow.result()["notified"] == {"stood_in": True}


def test_bare_the_same_calls_really_change_the_crm_and_write_files(run_workflow):
    bare = run_workflow(W, "enrich", "bare")
    assert bare.tools("write") == [(name, "real") for name in WRITE_TOOLS]
    assert rows(bare) == [
        ("acc_1", "Acme", "smb", "2026-01-01", 87, "manufacturing"),
        ("acc_3", "Initech", "enterprise", "2026-06-01", 0, None),
    ]
    assert files(bare) == ["crm.sqlite3", "exports/smb.csv", "outbox.jsonl"]
    assert bare.result()["deleted"] == 1


@pytest.mark.parametrize("mode", MODES)
def test_read_tools_run_for_real_in_both_modes_and_the_call_sequence_is_the_same(
    run_workflow, mode
):
    bare = run_workflow(W, "enrich", "bare")
    shadow = run_workflow(W, "enrich", mode)
    assert [n for n, _ in bare.tools()] == [n for n, _ in shadow.tools()]
    assert {ran for _, ran in shadow.tools("read")} == {"real"}
    # Decimal and datetime came back from the read tool as themselves: not JSON, not revivable
    # once recorded, so a replay must run `score_account` for real.
    assert {tuple(e["types"]) for e in shadow.events("scored")} == {("Decimal", "datetime")}


@pytest.mark.parametrize("mode", MODES)
def test_http_made_inside_a_read_tool_is_an_ordinary_exchange_of_the_run(run_workflow, mode):
    shadow = run_workflow(W, "enrich", mode)
    assert shadow.exchange_lines() == [
        "real          read      GET enrich.internal/v1/companies/acc_1 -> 200",
        "real          read      GET enrich.internal/v1/companies/acc_2 -> 200",
    ]
    # Each exchange is in the run the tool was called in, with no link to the call, and
    # neither count holds a tool call.
    [run] = shadow.sdk_runs()
    stored = shadow.stored().load_run(run.run_id).events
    assert [e.request.path for e in stored if not isinstance(e, ToolCall)] == [
        "/v1/companies/acc_1",
        "/v1/companies/acc_2",
    ]
    assert TWO_REAL in shadow.summaries()


@pytest.mark.parametrize("mode", MODES)
def test_irimi_prints_every_tool_call_and_a_summary_line_for_each_stood_in_write(
    run_workflow, mode
):
    """In the stream, one line per call in `exchange_line`'s columns, among the exchanges; in the
    summary, one line per write tool whose stand-in ran, after the writes, and none for a read.
    The run's only writes are tool writes, and the summary still says they did not happen."""
    shadow = run_workflow(W, "enrich", mode)
    assert shadow.tool_call_lines() == [
        "tool-real     read      crm.connect_with -> ok",
        "tool-real     read      crm.find_accounts -> ok",
        "tool-real     read      crm.fetch_enrichment -> ok",
        "tool-real     read      crm.score_account -> ok",
        "tool-fake     write     crm.upsert_account -> ok",
        "tool-real     read      crm.fetch_enrichment -> ok",
        "tool-real     read      crm.score_account -> ok",
        "tool-fake     write     crm.upsert_account -> ok",
        "tool-fake     write     crm.bulk_update -> ok",
        "tool-fake     write     crm.delete_stale -> ok",
        "tool-fake     write     crm.export_csv -> ok",
        "tool-fake     write     crm.notify_owner -> ok",
    ]
    summary = shadow.summaries()
    assert summary[1 : summary.index("  enrich.internal  2 reads")] == [
        "",
        "  None of these writes reached the real service:",
        *[f"  ○ tool  {name}  fake ran instead" for name in WRITE_TOOLS],
        "",
    ]
    assert TWO_REAL in summary


@pytest.mark.parametrize("mode", MODES)
def test_every_tool_call_is_stored_in_its_run_with_what_ran_and_what_it_returned(
    run_workflow, mode
):
    shadow = run_workflow(W, "enrich", mode)
    [run] = shadow.sdk_runs()
    calls = tool_calls(shadow)
    assert {call.run_id for call in calls} == {run.run_id}
    # What the run's own log says ran is what irimi stored, call for call.
    assert [(c.name, c.ran) for c in calls] == shadow.tools()
    assert {(c.kind, c.ran) for c in calls} == {("read", "real"), ("write", "shadow")}
    assert [c.name for c in calls if c.kind == "write"] == WRITE_TOOLS
    by_name = {c.name: c for c in calls}
    # Args are bound against the real function; the result is the stand-in's.
    assert by_name["crm.export_csv"].args == {"rows": SEED_SMB, "name": "smb"}
    assert by_name["crm.export_csv"].result == "<not written: smb.csv>"
    assert by_name["crm.notify_owner"].result == {"stood_in": True}
    assert by_name["crm.delete_stale"].result == 0
    # Values JSON cannot hold are stored as their repr, which a replay cannot revive.
    assert {k: set(v) for k, v in by_name["crm.score_account"].result.items()} == {
        "score": {REPR_KEY},
        "scored_at": {REPR_KEY},
    }


@pytest.mark.parametrize("mode", MODES)
def test_a_credential_in_a_tool_s_args_is_stored_as_its_placeholder(run_workflow, mode):
    """`connect_with` is a read tool given a DSN that carries a key: its stored args are redacted,
    and only the key is. Universal invariant 4 holds the whole home to it too."""
    shadow = run_workflow(W, "enrich", mode)
    [connect] = [c for c in tool_calls(shadow) if c.name == "crm.connect_with"]
    dsn = connect.args["dsn"]
    assert CANARIES["STRIPE_API_KEY"] not in dsn
    assert dsn.startswith("postgres://crm:<redacted:") and dsn.endswith(">@db.internal/crm")
    assert connect.result == {"connected": "db.internal/crm"}


@pytest.mark.parametrize("mode", MODES)
def test_a_read_tool_after_a_stood_in_write_sees_the_old_row(run_workflow, mode):
    # No tool overlay exists: the stand-in wrote nothing, so the read tool shows the old row
    # under shadow while bare shows the rename. This is the gap, pinned on purpose. The write no
    # label covers is real in every mode, so the second read shows its `last_seen`.
    bare = run_workflow(W, "read_after_write", "bare")
    assert bare.result()["saw_rename"] is True
    shadow = run_workflow(W, "read_after_write", mode)
    assert shadow.result()["saw_rename"] is False
    assert (
        shadow.result()["last_seen"] == bare.result()["last_seen"] == ["2026-01-01", "2026-09-28"]
    )
    assert rows(shadow) == TOUCHED


@pytest.mark.parametrize("mode", ("bare", *MODES))
def test_a_stale_irimi_replay_run_asks_no_recording_but_where_irimi_cannot_remove_it(
    run_workflow, mode
):
    """`read_after_write` runs from a shell that exported a stale `IRIMI_REPLAY_RUN`.
    `irimi shadow` removes it from its child, so its SDK never asks the recording. A bare agent
    keeps it, and its SDK is inert, so it asks nothing either: no control request reaches the fake
    internet (`test_control_endpoint.py`). A serve agent is not irimi's child and keeps it too, but
    names no `IRIMI_RUN`, which serve never sets, and the SDK takes a replay's run only when irimi
    named it as both: so it asks nothing, adopts nothing, and the read runs for real."""
    result = run_workflow(W, "read_after_write", mode)
    expected = None if mode == "shadow" else STALE_REPLAY_RUN
    assert [start["replay"] for start in result.events("start")] == [expected]
    assert result.tools("read") == [("crm.find_accounts", "real")] * 2
    if mode != "bare":  # bare, the one warning is the real write's (`test_control_endpoint.py`)
        assert result.events("log") == []
        # The run is the SDK's own, started and stored, never the stale id adopted.
        [start] = result.events("run.start")
        assert start["run"] != STALE_REPLAY_RUN
        assert [(r.run_id, r.attribution) for r in result.sdk_runs()] == [(start["run"], "sdk")]


@pytest.mark.parametrize("mode", MODES)
def test_an_unlabelled_database_write_happens_for_real_under_shadow(run_workflow, mode):
    # irimi cannot see a write that is neither HTTP nor an @sdk.tool. A future check that flags a
    # production database URL in the agent's environment would flip this assertion.
    shadow = run_workflow(W, "unlabeled_write", mode)
    assert rows(shadow)[0] == ("acc_1", "Acme", "smb", "2026-09-28", 0, None)
    assert shadow.exchange_lines() == []
    assert rows(run_workflow(W, "unlabeled_write", "bare")) == rows(shadow)


@pytest.mark.parametrize("mode", MODES)
def test_a_raising_read_tool_ends_the_run_in_error_under_shadow(run_workflow, mode):
    shadow = run_workflow(W, "tool_raises", mode)
    bare = run_workflow(W, "tool_raises", "bare")
    assert shadow.exit_code == bare.exit_code == 1
    for result in (shadow, bare):
        assert [e["key"] for e in result.events("lookup_failed")] == ["'acc_1'"]
    ends = shadow.events("run.end")
    assert [(e["outcome"], e["error"]) for e in ends] == [("error", "KeyError")]
    # Stored under the exception's full name.
    [run] = shadow.sdk_runs()
    assert (run.outcome, run.error) == ("error", ErrorInfo("builtins.KeyError", "'acc_missing'"))
    # Every tool that raised is recorded with what it raised and no result, the caught ones and
    # the one that ended the run.
    assert [(c.name, c.result, c.error) for c in tool_calls(shadow)] == [
        ("crm.connect_with", None, connect_error(shadow)),
        ("crm.flaky_lookup", None, ErrorInfo("builtins.KeyError", "'acc_1'")),
        ("crm.score_account", None, ErrorInfo("builtins.KeyError", "'acc_missing'")),
    ]
    assert shadow.tool_call_lines() == [
        "tool-real     read      crm.connect_with -> raised builtins.ConnectionError",
        "tool-real     read      crm.flaky_lookup -> raised builtins.KeyError",
        "tool-real     read      crm.score_account -> raised builtins.KeyError",
    ]
    # Bare, the SDK is inactive: no run is opened at all.
    assert bare.events("run.start") == []


def connect_error(shadow) -> ErrorInfo:
    """What a trace keeps of the error `connect_with` raised in `tool_raises`, which quotes its
    DSN's key across the 1000-character cut: the SDK posts the message whole, and the store
    redacts it whole before it cuts it, so the key is its whole placeholder."""
    key = CANARIES["STRIPE_API_KEY"]
    said = f"{CONNECT_RETRIES}could not connect with postgres://crm:{key}@{UNREACHABLE_HOST}/crm"
    assert said.index(key) < MAX_ERROR_MESSAGE < said.index(key) + len(key)
    hidden = redact.placeholder(redact.load_key(shadow.home), key)
    return ErrorInfo("builtins.ConnectionError", said.replace(key, hidden)[:MAX_ERROR_MESSAGE])


@pytest.mark.parametrize("mode", MODES)
def test_an_sdk_error_quoting_a_key_across_the_cut_is_stored_as_its_placeholder(run_workflow, mode):
    """`connect_with` raises a message that quotes its DSN, the key in it across the 1000
    characters a trace keeps. The SDK posts it whole and the store redacts it whole, then cuts
    it, so the stored message is a prefix of the redacted whole: the key's own placeholder, the
    one its args hold, and no part of the key. Cut first, the part kept was redacted as a key of
    its own. The agent caught it, so it ran on, in every mode."""
    shadow = run_workflow(W, "tool_raises", mode)
    [connect] = [c for c in tool_calls(shadow) if c.name == "crm.connect_with"]
    hidden = redact.placeholder(redact.load_key(shadow.home), CANARIES["STRIPE_API_KEY"])
    assert connect.error == connect_error(shadow)
    assert f"could not connect with postgres://crm:{hidden}@" in connect.error.message
    assert connect.args["dsn"] == f"postgres://crm:{hidden}@{UNREACHABLE_HOST}/crm"
    for result in (shadow, run_workflow(W, "tool_raises", "bare")):
        assert result.events("connect_failed", with_time=False) == [
            {"event": "connect_failed", "error": "ConnectionError"}
        ]


@pytest.mark.parametrize("mode", MODES)
def test_a_repr_quoting_a_key_across_the_sdks_cut_keeps_no_part_of_it(run_workflow, mode):
    """`connect_with`'s options are captured as their repr, which quotes a key across the 1000
    characters the SDK keeps of one. The SDK cuts before irimi redacts, so it cuts where the key
    starts, and keeps everything before it: cut at 1000, the 15 characters of the key it kept
    were no longer its shape, and reached disk as they were, which universal invariant 4 finds.
    The store's copy is the SDK's, so it holds no placeholder either."""
    shadow = run_workflow(W, "tool_raises", mode)
    [connect] = [c for c in tool_calls(shadow) if c.name == "crm.connect_with"]
    key = CANARIES["OPENAI_API_KEY"]
    said = repr(DriverOptions(key))
    assert said.index(key) < MAX_REPR < said.index(key) + len(key)
    assert connect.args["options"] == {REPR_KEY: said[: said.index(key)]}
    assert said[: said.index(key)].endswith("', password='")
    on_disk = [p.read_bytes() for p in shadow.store_root().rglob("*") if p.is_file()]
    assert not any(key[:12].encode() in data for data in on_disk)
    assert check_invariants(shadow) == []


@pytest.mark.parametrize("mode", MODES)
def test_each_decoration_time_mistake_raises_type_error(run_workflow, mode):
    shadow = run_workflow(W, "decoration_errors", mode)
    assert shadow.result()["raised"] == [
        "write_without_shadow",
        "read_with_shadow",
        "bad_kind",
        "stand_in_not_callable",
        "sync_stand_in_for_async",
        "async_stand_in_for_sync",
        "generator",
        "generator_stand_in",
        "stand_in_is_the_function",
    ]
    assert (
        run_workflow(W, "decoration_errors", "bare").result()["raised"]
        == (shadow.result()["raised"])
    )


@pytest.mark.parametrize("mode", MODES)
def test_a_write_stand_in_that_raises_hands_the_agent_its_own_exception_and_a_retry_runs(
    run_workflow,
    mode,
):
    """`merge_accounts` first names an account that does not exist. Its stand-in validates as the
    real merge does and raises; the SDK records the call with its error and re-raises the very
    object the stand-in raised, so the agent catches it and retries with the right account.
    Both calls are recorded and printed, the first with what it raised; the summary says the
    stand-in raised, and the run, which caught it, ends `ok`."""
    shadow = run_workflow(W, "merge_retry", mode)
    bare = run_workflow(W, "merge_retry", "bare")
    for result in (shadow, bare):
        failed = [(e["error"], e["same_object"]) for e in result.events("merge_failed")]
        assert failed == [("no such account: acc_9", True)]
    assert shadow.tools() == [("crm.merge_accounts", "shadow")] * 2
    assert bare.tools() == [("crm.merge_accounts", "real")] * 2
    assert rows(shadow) == SEED
    assert [row[0] for row in rows(bare)] == ["acc_1", "acc_3"]
    assert shadow.result()["merged"] == {"kept": "acc_1", "merged": "acc_2", "stood_in": True}
    assert bare.result()["merged"] == {"kept": "acc_1", "merged": "acc_2"}
    [run] = shadow.sdk_runs()
    assert run.outcome == "ok"
    # Args are bound against the real `merge_accounts(keep, drop)`, not the stand-in's `*ids`.
    assert [(c.run_id, c.args, c.result, c.error) for c in tool_calls(shadow)] == [
        (
            run.run_id,
            {"keep": "acc_1", "drop": "acc_9"},
            None,
            ErrorInfo("builtins.LookupError", "no such account: acc_9"),
        ),
        (
            run.run_id,
            {"keep": "acc_1", "drop": "acc_2"},
            {"kept": "acc_1", "merged": "acc_2", "stood_in": True},
            None,
        ),
    ]
    assert shadow.tool_call_lines() == [
        "tool-fake     write     crm.merge_accounts -> raised builtins.LookupError",
        "tool-fake     write     crm.merge_accounts -> ok",
    ]
    summary = shadow.summaries()
    totals = "  0 exchanges · 0 real · 0 answered by irimi (0 faked writes) · 0 sent to targets"
    assert summary[1 : summary.index(totals) + 1] == [
        "",
        "  None of these writes reached the real service:",
        "  ○ tool  crm.merge_accounts  fake raised builtins.LookupError",
        "  ○ tool  crm.merge_accounts  fake ran instead",
        "",
        totals,
    ]


def test_under_serve_a_tool_called_outside_every_run_is_unattributed(run_workflow):
    """Serve mode has no process run: the housekeeping tools called in no run post to
    `runs/unattributed/tool-calls`, which the control endpoint takes for tool calls only,
    and land in `unattributed/`. The trigger's own calls are its run's, and only its stood-in
    write is in its summary."""
    serve = run_workflow(W, "outside_run", "serve")
    [run] = serve.sdk_runs()
    reader = serve.stored()
    assert [(c.name, c.ran, c.args) for c in reader.load_run(UNATTRIBUTED).events] == [
        ("crm.delete_stale", "shadow", {"cutoff": "2025-06-01"}),
        ("crm.find_accounts", "real", {"segment": "smb"}),
    ]
    assert [(c.name, c.ran) for c in reader.load_run(run.run_id).events] == [
        ("crm.find_accounts", "real"),
        ("crm.upsert_account", "shadow"),
    ]
    assert [line for line in serve.run_blocks()[run.run_id] if "○ tool" in line] == [
        "  ○ tool  crm.upsert_account  fake ran instead"
    ]
    assert len(serve.tool_call_lines()) == 4
    assert rows(serve) == SEED


def test_a_tool_called_outside_every_run_lands_in_the_process_run(run_workflow):
    """Housekeeping before the trigger: `delete_stale` and `find_accounts` are called in no run,
    so under `irimi shadow -- <cmd>` each is recorded in the process run that `IRIMI_RUN` names,
    and printed like any other. The trigger's own calls are its run's. Nothing is left
    `unattributed`."""
    shadow = run_workflow(W, "outside_run", "shadow")
    bare = run_workflow(W, "outside_run", "bare")
    [run] = shadow.sdk_runs()
    process = shadow.process_run_id()
    assert [(t["name"], t["ran"], t["run"]) for t in shadow.events("tool")] == [
        ("crm.delete_stale", "shadow", None),
        ("crm.find_accounts", "real", None),
        ("crm.find_accounts", "real", run.run_id),
        ("crm.upsert_account", "shadow", run.run_id),
    ]
    reader = shadow.stored()

    def held(run_id):
        return [(c.name, c.ran, c.args) for c in reader.load_run(run_id).events]

    assert held(process) == [
        ("crm.delete_stale", "shadow", {"cutoff": "2025-06-01"}),
        ("crm.find_accounts", "real", {"segment": "smb"}),
    ]
    assert [(name, ran) for name, ran, _ in held(run.run_id)] == [
        ("crm.find_accounts", "real"),
        ("crm.upsert_account", "shadow"),
    ]
    assert reader.load_run(UNATTRIBUTED).events == []
    assert shadow.tool_call_lines() == [
        "tool-fake     write     crm.delete_stale -> ok",
        "tool-real     read      crm.find_accounts -> ok",
        "tool-real     read      crm.find_accounts -> ok",
        "tool-fake     write     crm.upsert_account -> ok",
    ]
    assert [line for line in shadow.summary() if "○ tool" in line] == [
        "  ○ tool  crm.delete_stale  fake ran instead",
        "  ○ tool  crm.upsert_account  fake ran instead",
    ]
    # The read after the stood-in delete still sees the stale account: no tool overlay, as in
    # `read_after_write`. Bare, it was really deleted.
    assert [(e["stale"], e["smb"]) for e in shadow.events("housekeeping")] == [(0, 2)]
    assert [(e["stale"], e["smb"]) for e in bare.events("housekeeping")] == [(1, 1)]
    assert rows(shadow) == SEED
    assert rows(bare) == [
        ("acc_1", "Acme", "smb", "2026-01-01", 50, None),
        ("acc_3", "Initech", "enterprise", "2026-06-01", 0, None),
    ]


@pytest.mark.parametrize("mode", MODES)
def test_http_made_inside_a_write_stand_in_is_an_ordinary_exchange_of_the_run(run_workflow, mode):
    """`sync_industry`'s stand-in checks the account upstream before saying what it would have
    written. Its request is the run's like any other: labelled by the SDK, forwarded, printed and
    stored in the run, before the tool call, which is posted once the stand-in returned, with no
    link between them. Bare, the real body writes the row and makes no request at all."""
    shadow = run_workflow(W, "stand_in_http", mode)
    bare = run_workflow(W, "stand_in_http", "bare")
    [run] = shadow.sdk_runs()
    assert [(c["label"], c["status"], c["answered_by"], c["run"]) for c in shadow.calls()] == [
        ("stand_in_check", 200, None, run.run_id)
    ]
    assert shadow.exchange_lines() == [
        "real          read      GET enrich.internal/v1/companies/acc_1 -> 200"
    ]
    assert shadow.tool_call_lines() == ["tool-fake     write     crm.sync_industry -> ok"]
    events = shadow.stored().load_run(run.run_id).events
    assert [type(e).__name__ for e in events] == ["Exchange", "ToolCall"]
    assert events[0].request.path == "/v1/companies/acc_1"
    assert events[1].result == {
        "stood_in": True,
        "industry": "aerospace",
        "upstream": "manufacturing",
    }
    assert "  ○ tool  crm.sync_industry  fake ran instead" in shadow.summaries()
    assert rows(shadow) == SEED
    assert bare.calls() == []
    assert rows(bare)[0] == ("acc_1", "Acme", "smb", "2026-01-01", 0, "aerospace")


@pytest.mark.parametrize("mode", MODES)
def test_a_sync_tool_that_returns_a_coroutine_is_recorded_when_the_coroutine_ends(
    run_workflow, mode
):
    """`schedule_followup` is sync and returns its work as a coroutine; its stand-in does the same.
    The call ends when the coroutine does, so the read tool the coroutine awaits is stored
    before it, though the agent called it after, and the stored result is what the coroutine
    returned, not the coroutine. `lookup_owner` is an async read tool, awaited in the run's event
    loop. Last, `Outbox.send_reminder`, an async static method whose stand-in is a static method
    in the same class body: judged as the async function inside it, so accepted, and run in the
    write's place. `last_contact` is sync and returns a coroutine too, which the agent leaves
    pending while `find_accounts` runs, so the find is stored first and within its span."""
    shadow = run_workflow(W, "async_tools", mode)
    bare = run_workflow(W, "async_tools", "bare")
    assert shadow.tools() == [
        ("crm.lookup_owner", "real"),
        ("crm.last_contact", "real"),
        ("crm.find_accounts", "real"),
        ("crm.schedule_followup", "shadow"),
        ("crm.lookup_owner", "real"),
        ("crm.send_reminder", "shadow"),
    ]
    [run] = shadow.sdk_runs()
    first, found, contact, inner, schedule, remind = shadow.stored().load_run(run.run_id).events
    calls = (first, found, contact, inner, schedule, remind)
    assert [(c.name, c.ran, c.awaited) for c in calls] == [
        ("crm.lookup_owner", "real", False),
        ("crm.find_accounts", "real", False),
        ("crm.last_contact", "real", True),
        ("crm.lookup_owner", "real", False),
        ("crm.schedule_followup", "shadow", True),
        ("crm.send_reminder", "shadow", False),
    ]
    # `last_contact` is stored with what the coroutine returned, and `find_accounts` within it.
    assert contact.result == {"id": "acc_1", "last_seen": "2026-01-01"}
    assert contact.started_at <= found.started_at <= found.ended_at <= contact.ended_at
    assert schedule.args == {"account_id": "acc_1", "days": 7}
    owner = "owner@acme.example"
    assert schedule.result == {"stood_in": True, "owner": owner, "in_days": 7}
    assert inner.result == first.result == {"id": "acc_1", "owner": owner}
    assert schedule.started_at <= inner.started_at <= inner.ended_at <= schedule.ended_at
    assert (remind.args, remind.result) == (
        {"account_id": "acc_1", "owner": owner},
        {"stood_in": True, "to": owner},
    )
    assert shadow.tool_call_lines() == [
        "tool-real     read      crm.lookup_owner -> ok",
        "tool-real     read      crm.find_accounts -> ok",
        "tool-real     read      crm.last_contact -> ok",
        "tool-real     read      crm.lookup_owner -> ok",
        "tool-fake     write     crm.schedule_followup -> ok",
        "tool-fake     write     crm.send_reminder -> ok",
    ]
    assert [line for line in shadow.summaries() if "○ tool" in line] == [
        "  ○ tool  crm.schedule_followup  fake ran instead",
        "  ○ tool  crm.send_reminder  fake ran instead",
    ]
    assert shadow.result()["scheduled"] == {"stood_in": True, "owner": owner, "in_days": 7}
    assert shadow.result()["reminded"] == {"stood_in": True, "to": owner}
    assert shadow.result()["last_seen"] == bare.result()["last_seen"] == "2026-01-01"
    assert shadow.result()["segment"] == bare.result()["segment"] == ["acc_1", "acc_2"]
    assert files(shadow) == ["crm.sqlite3"]
    assert bare.result()["scheduled"] == {"scheduled": "acc_1", "owner": owner, "in_days": 7}
    assert bare.result()["reminded"] == {"sent": "acc_1", "to": owner}
    assert files(bare) == ["crm.sqlite3", "followups.jsonl", "outbox.jsonl"]


# ---------------------------------------------------------------------- irimi compare

ENRICH = "enrich_accounts  examples.workflows.w06_crm_db_agent.agent:enrich_accounts"


@pytest.mark.parametrize("mode", MODES)
def test_irimi_compare_shows_the_retry_s_failed_merge_as_the_one_it_added(shared_store_run, mode):
    """`merge_once` makes the merge right the first time; `merge_retry` makes it after a first
    call whose stand-in raised, each recorded into one store, B under shadow or serve.
    Compared, the call both made, the stand-in that ran, is the same, and the one that raised is
    the one B added: the same tool, so lining the two up by name alone would have paired the merge
    that ran with the one that raised and called it changed (compare pairs identical calls before
    it pairs by name)."""
    once = shared_store_run(W, "merge_once")
    retry = shared_store_run(W, "merge_retry", mode)
    assert check_invariants(retry) == []
    code, out, err = compare(once, retry)
    a, b = (result.one("run.start")["run"] for result in (once, retry))
    assert (code, err) == (1, [])
    assert out == [
        f"irimi compare · {a} vs {b}",
        f"  A  {ENRICH}",
        f"  B  {ENRICH}",
        "  (the triggers' args differ)",
        "",
        "  + ○ tool  crm.merge_accounts  fake raised builtins.LookupError",
        "  = ○ tool  crm.merge_accounts  fake ran instead",
        "",
        "  1 same · 0 changed · 0 only in A · 1 only in B",
    ]
    assert rows_not_shown(retry, a, b, out) == []


@pytest.mark.parametrize("mode", MODES)
def test_two_writes_at_once_are_compared_in_the_order_they_ended(run_workflow, mode):
    """`overlapping_writes`: the archive starts first and ends only after the flag has ended, so
    the run stores the flag first (`seq` is the order calls ended). The summary prints the
    two in that order, and so does `irimi compare`, which lines writes up in `seq` order and not
    in the order they started."""
    shadow = run_workflow(W, "overlapping_writes", mode)
    bare = run_workflow(W, "overlapping_writes", "bare")
    assert bare.result()["flagged"] == {"flagged": True}
    assert bare.result()["archived"] == {"archived": True}
    flag, archive = tool_calls(shadow)
    assert (flag.name, archive.name) == ("crm.flag_account", "crm.archive_account")
    assert archive.started_at < flag.started_at <= flag.ended_at <= archive.ended_at
    [run] = shadow.sdk_runs()
    rows = [
        "  = ○ tool  crm.flag_account  fake ran instead",
        "  = ○ tool  crm.archive_account  fake ran instead",
    ]
    assert shadow.compare_runs(run.run_id, run.run_id) == (
        0,
        [
            f"irimi compare · {run.run_id} vs {run.run_id}",
            f"  A  {ENRICH}",
            f"  B  {ENRICH}",
            "",
            *rows,
            "",
            "  2 same · 0 changed · 0 only in A · 0 only in B",
        ],
        [],
    )
    assert [line for line in shadow.summaries() if "○ tool" in line] == [f"  {r[4:]}" for r in rows]


# ------------------------------------------------------------------------------- replay


def replayed_calls(replay) -> list[ToolCall]:
    """The tool calls the replay's own engine stored, in `seq` order: not the recording's, which
    the replay's store holds too."""
    return [e for e in replay.handed_events if isinstance(e, ToolCall)]


def test_enrich_replayed_answers_its_read_tools_from_the_recording(replayed):
    """Each read tool the recording holds is answered from it, and its real body never runs (the
    corpus's log says which body ran). `score_account`'s recorded result holds `Decimal` and
    `datetime` as their repr, which does not revive, so it runs for real: the replay's one miss.
    Every write runs its stand-in, as in the recording, and nothing reaches the fake internet:
    `fetch_enrichment`'s GET is never made, since the tool around it is answered. The trigger is
    called alone, so the agent's `main()` never seeds its database, which no tool needs."""
    recorded, replay = replayed(W, "enrich")
    assert replay.exit_code == recorded.exit_code == replay.agent_exit_code() == 0
    assert [name for name, _ in replay.tools()] == [name for name, _ in recorded.tools()]
    per_account = [
        ("crm.fetch_enrichment", "recording"),
        ("crm.score_account", "real"),
        ("crm.upsert_account", "shadow"),
    ]
    assert replay.tools() == [
        ("crm.connect_with", "recording"),
        ("crm.find_accounts", "recording"),
        *per_account,
        *per_account,
        ("crm.bulk_update", "shadow"),
        ("crm.delete_stale", "shadow"),
        ("crm.export_csv", "shadow"),
        ("crm.notify_owner", "shadow"),
    ]
    assert [(c.name, c.ran) for c in replayed_calls(replay)] == replay.tools()
    assert replay.internet.requests() == []
    assert replay.exchange_lines() == []
    # The agent got what it got the first time, the revived values and the real ones alike.
    assert replay.events("scored", with_time=False) == recorded.events("scored", with_time=False)
    assert replay.returned() == recorded.returned()
    assert not (replay.state / DB_NAME).exists()


def test_enrich_replayed_where_no_lookup_reaches_irimi_runs_each_read_for_real(replayed):
    """The agent reaches irimi's control endpoint through a gateway that refuses the route a read
    tool asks its recorded result on (`agentkit.lookup_gateway`), and forwards every post. Each
    lookup fails, so each read tool is a miss and runs for real, `ran="real"`, and the SDK says so
    once, on its logger, for six failed lookups; the agent is unharmed and ends as recorded, every
    call is still recorded, and nothing reaches the fake internet: `fetch_enrichment`'s body runs,
    and its GET, which the tool's recorded call did not use up, is answered from the recording.
    Replayed by its command: the gateway and the logger the warning reaches are the
    agent's `main()`'s, which a trigger called alone never runs."""
    recorded, replay = replayed(W, "enrich", {LOOKUP_REFUSED_ENV: "1"}, command=True)
    assert replay.exit_code == recorded.exit_code == replay.agent_exit_code() == 0
    assert replay.result(with_time=False) == recorded.result(with_time=False)
    assert replay.tools("read") == [(name, "real") for name, _ in recorded.tools("read")]
    assert len(replay.tools("read")) == 6
    assert replay.tools("write") == recorded.tools("write")
    assert [(c.name, c.ran) for c in replayed_calls(replay)] == replay.tools()
    [warned] = replay.events("log", with_time=False)
    assert (warned["logger"], warned["level"]) == ("irimi.sdk", "WARNING")
    assert warned["message"].startswith(
        "irimi could not look up a recorded call of tool crm.connect_with in run "
    )
    assert "(rejected: HTTP 502: " in warned["message"]
    assert warned["message"].endswith(
        "The tool runs for real; further failures of this kind are not logged."
    )
    assert replay.exchange_lines() == [
        f"recorded      read      GET enrich.internal/v1/companies/{account} -> 200  "
        "[from-recording]"
        for account in ("acc_1", "acc_2")
    ]
    assert replay.internet.requests() == []
    assert rows(replay) == SEED


def test_a_credential_in_a_replayed_tool_s_args_matches_its_placeholder(replayed):
    """`connect_with` is called with the real key in its DSN, and the recording holds the DSN
    with the key's placeholder: they match only once the live args are redacted with the store's
    key, as the recording's were."""
    recorded, replay = replayed(W, "enrich")
    [stored] = [c for c in tool_calls(recorded) if c.name == "crm.connect_with"]
    assert CANARIES["STRIPE_API_KEY"] not in stored.args["dsn"]
    [again] = [
        c for c in tool_calls(replay) if (c.name, c.ran) == ("crm.connect_with", "recording")
    ]
    assert (again.args, again.result) == (stored.args, stored.result)


def test_read_after_write_replayed_matches_both_reads_in_order(replayed):
    """Both `find_accounts("smb")` calls take the two recorded results, in the order they were
    recorded, so the replay shows the old row, as shadow did. The stand-in ran between them, and
    with no tool overlay neither recorded read showed it. Replayed by its command:
    the write no label covers runs for real, against the database the agent's `main()` creates."""
    recorded, replay = replayed(W, "read_after_write", command=True)
    assert replay.tools() == [
        ("crm.find_accounts", "recording"),
        ("crm.upsert_account", "shadow"),
        ("crm.find_accounts", "recording"),
    ]
    assert replay.result()["saw_rename"] is recorded.result()["saw_rename"] is False
    # The write no label covers made the two recorded results differ: the first read got the
    # first, untouched, and the second the second.
    assert replay.result()["last_seen"] == recorded.result()["last_seen"]
    assert recorded.result()["last_seen"] == ["2026-01-01", "2026-09-28"]
    first, second = (c for c in replayed_calls(replay) if c.name == "crm.find_accounts")
    assert [first.result[0]["last_seen"], second.result[0]["last_seen"]] == [
        "2026-01-01",
        "2026-09-28",
    ]
    # The stale `IRIMI_REPLAY_RUN` the scenario exported is irimi's own run in a replay.
    assert [start["replay"] for start in replay.events("start")] == [replay.process_run_id()]


def test_tool_raises_replayed_raises_what_was_recorded_and_the_agent_catches_it_again(replayed):
    """`flaky_lookup`'s recorded KeyError comes back as `sdk.RecordedToolError`, a subclass of
    KeyError named KeyError, so the agent's `except KeyError` catches it and logs what it logged
    the first time. `score_account`'s ends the run where it ended, and the trigger, called alone,
    raises out of `replay_child`, which exits 1 as the agent did."""
    recorded, replay = replayed(W, "tool_raises")
    assert replay.agent_exit_code() == recorded.exit_code == 1
    assert replay.exit_code == 0
    assert replay.events("lookup_failed", with_time=False) == [
        {"event": "lookup_failed", "error": "KeyError", "key": "'acc_1'"}
    ]
    assert replay.tool_call_lines() == [
        "tool-recorded read      crm.connect_with -> raised builtins.ConnectionError",
        "tool-recorded read      crm.flaky_lookup -> raised builtins.KeyError",
        "tool-recorded read      crm.score_account -> raised builtins.KeyError",
    ]
    assert [(c.name, c.error) for c in replayed_calls(replay)] == [
        ("crm.connect_with", connect_error(recorded)),
        ("crm.flaky_lookup", ErrorInfo("builtins.KeyError", "'acc_1'")),
        ("crm.score_account", ErrorInfo("builtins.KeyError", "'acc_missing'")),
    ]
    # And the run ends on the recorded type, not on the class it was raised again as: the
    # replay's run posts no end of its own, and `irimi replay` ends it with what `replay_child`
    # said its call raised.
    assert replay.one("run.end")["error"] == recorded.one("run.end")["error"] == "KeyError"
    [source] = recorded.sdk_runs()
    record = replay.stored().load_run(replay.process_run_id()).record
    assert (record.outcome, record.error) == (source.outcome, source.error)
    assert record.error == ErrorInfo("builtins.KeyError", "'acc_missing'")


def test_async_tools_replayed_answers_each_read_with_what_its_await_got(replayed):
    """`lookup_owner` is an async read, and `last_contact` a sync read whose recorded call
    returned a coroutine (`awaited`): the recording answers the first with a coroutine function's
    result and the second with a coroutine, so the agent's `await` gets what it got when recorded,
    and neither real body runs. `lookup_owner` is called a second time inside the stand-in of the
    sync write `schedule_followup`, and the recording answers it there too.

    `find_accounts` was recorded within `last_contact`'s span, the agent having left its coroutine
    pending, though `last_contact`'s body never called it. Answering `last_contact` does not use
    it up, since no later call of it would take its record in its place: it is answered too."""
    recorded, replay = replayed(W, "async_tools")
    assert replay.exit_code == recorded.exit_code == replay.agent_exit_code() == 0
    assert replay.tools() == [
        ("crm.lookup_owner", "recording"),
        ("crm.last_contact", "recording"),
        ("crm.find_accounts", "recording"),
        ("crm.schedule_followup", "shadow"),
        ("crm.lookup_owner", "recording"),
        ("crm.send_reminder", "shadow"),
    ]
    calls = replayed_calls(replay)
    assert [(c.name, c.ran, c.awaited) for c in calls] == [
        ("crm.lookup_owner", "recording", False),
        ("crm.find_accounts", "recording", False),
        ("crm.last_contact", "recording", True),
        ("crm.lookup_owner", "recording", False),
        ("crm.schedule_followup", "shadow", True),
        ("crm.send_reminder", "shadow", False),
    ]
    recorded_calls = [e for e in recorded.handed_events if isinstance(e, ToolCall)]
    assert [(c.name, c.args, c.result) for c in calls] == [
        (c.name, c.args, c.result) for c in recorded_calls
    ]
    assert replay.returned() == recorded.returned()
