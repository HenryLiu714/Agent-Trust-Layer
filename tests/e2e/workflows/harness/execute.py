"""Run a scenario in one of four modes, with the fake internet answering every host.

- bare: the agent as a plain subprocess, its proxy the fake internet, so its writes land there;
- shadow: `irimi shadow -- <agent>` in this process, with irimi's real shipped maps and policy;
- serve: `irimi serve` on a thread of this process, and the agent beside it as its own process;
- replay: `irimi replay <run>` (or `testing.replay`) in this process, over a copy of a run's store.
"""

from __future__ import annotations

import contextlib
import functools
import io
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from unittest import mock

from irimi import testing
from irimi.cli import compose, parse_args, runner
from irimi.cli import main as irimi_main
from irimi.cli.compose import EngineFactory
from irimi.config import ca
from irimi.core.exchange import Exchange
from irimi.engine import Engine, OnExchange, OnTlsFailure
from irimi.engine.control import OnToolCall
from irimi.protocol.env import CONTROL_ENV, ENGINE_ACTIVE_ENV, PROXY_ENVS
from irimi.protocol.replay_files import replay_result_from_json, replay_result_to_json
from irimi.protocol.trace import ToolCall
from irimi.record.store import writer as store_writer
from tests.e2e.workflows.harness.internet import FakeInternet
from tests.e2e.workflows.harness.invariants import CANARIES
from tests.e2e.workflows.harness.result import Result
from tests.e2e.workflows.harness.scenario import (
    REPO_ROOT,
    Scenario,
    Workflow,
    environ,
    scenario_maps,
)
from tests.e2e.workflows.harness.services import World

MODES = ("bare", "shadow", "serve")
# How long a serve-mode run waits for irimi's banner, and then for irimi to stop.
SERVE_TIMEOUT_S = 60
# What the name of a thread the fake internet answers a request on ends in: the one kind of
# thread a serve run may leave behind it.
FAKE_HANDLER = "(process_request_thread)"
# How long the threads a serve started have to end once it has returned.
THREAD_EXIT_S = 5.0
PROXY_VARS = ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY", "all_proxy")
# The only variables an agent inherits from the harness's own environment. Everything else it is
# given by name, so a developer's shell cannot change a run: an exported `IRIMI_ENGINE_ACTIVE=1`
# would turn a bare run's write tools into stand-ins, and an exported `PROMPT` or
# `SLACK_POST_CHANNEL` would change what an agent sends.
INHERITED_ENV = ("PATH", "HOME", "USER", "LOGNAME", "LANG", "TZ", "SYSTEMROOT")


def run(
    workflow: Workflow, scenario_name: str, mode: str, workdir: Path, store: Path | None = None
) -> Result:
    """One scenario in one mode. `store` is passed to irimi as `--store`, so two runs can record
    into one store and be compared; without it irimi records into the run's own home."""
    if mode not in MODES:
        raise ValueError(f"mode must be one of {MODES}, not {mode!r}")
    return _execute(workflow, scenario_name, mode, workdir, store)


def replay(
    recorded: Result,
    run_id: str,
    workdir: Path,
    env: dict[str, str] | None = None,
    options: tuple[str, ...] = (),
    *,
    command: bool = False,
) -> Result:
    """`run_id`, a run `recorded` stored, replayed by `irimi replay <run_id>`:
    `recorded`'s scenario, recorded under `irimi shadow` or serve or itself a replay, in a
    copy of its `IRIMI_HOME` and its store under `workdir`, so `recorded` reads its store as it was
    and the replay's holds both, with `env` over the scenario's own environment - `PROMPT=half` is
    the prompt edit - and `options` given to `irimi replay` too (`--entrypoint`, `--args`). What
    runs is what `irimi replay` runs for the run, or, with `command`, the scenario's agent command.
    The result file `irimi replay` writes is the result's `replay`.

    Everything else is the run's own: a fresh `World` from the scenario's `setup` and a fresh fake
    internet, so a request the recording does not answer shows up in `internet.requests()`, and its
    own state, working directory and TMPDIR under `workdir`. The result's `mode` is `replay`."""
    return _replayed(recorded, run_id, workdir, env, _Replay(options, command))


def sdk_replay(recorded: Result, run_id: str, workdir: Path, *call: Any) -> Result:
    """`testing.replay(run_id, fn, args=args)` for `call` = `(fn, *args)` (or
    `testing.replay(run_id)` for none) called as a test calls it, a `replay` of `recorded` in every
    other way. `testing.replay`'s one seam, `testing._run_irimi`, runs `irimi` in this process
    instead of a subprocess, so the fake internet resolves its names and the scenario's maps load:
    the agent is still a child process of its own, `replay.child`, as `irimi replay` starts it.
    The result's `replay` is what `testing.replay` returned."""
    return _replayed(recorded, run_id, workdir, None, _Replay(call=call))


@dataclass(frozen=True)
class _Replay:
    """How `_execute` replays a run: the `options` `irimi replay` is given, whether it runs
    the scenario's `command`, and, for `sdk_replay`, the `testing.replay` `call` it makes
    instead."""

    options: tuple[str, ...] = ()
    command: bool = False
    call: tuple[Any, ...] | None = None


def _replayed(
    recorded: Result, run_id: str, workdir: Path, env: dict[str, str] | None, how: _Replay
) -> Result:
    # A run recorded under serve is replayed as one recorded under `irimi shadow`: `irimi replay`
    # reads only the store, and serve's runs are the ones a deployed agent records.
    if recorded.mode not in ("shadow", "serve", "replay"):
        raise ValueError(
            f"replay takes a run recorded under shadow or serve, or replayed, not {recorded.mode}"
        )
    # A replay records into the store it answers from, so it runs in a copy of `recorded`'s home,
    # and of the store it was given: never in `recorded`'s own, which a session-cached recording
    # or replay shares with every test that reads it. Otherwise a replay of the cached replay
    # writes its run there, and `test_replays.py`'s rule 9 fails for W1 `full_refund` whenever
    # W1's replay-of-a-replay tests run first.
    workdir.mkdir(parents=True, exist_ok=True)
    home = workdir / "recorded-home"
    shutil.copytree(recorded.home, home)
    store = None
    if recorded.store is not None:
        store = workdir / "recorded-store"
        shutil.copytree(recorded.store, store)
    return _execute(
        recorded.workflow,
        recorded.scenario,
        "replay",
        workdir,
        store,
        home=home,
        env=env or {},
        replay_of=run_id,
        how=how,
    )


def replayed_run_id(recorded: Result) -> str:
    """The run a replay of `recorded` answers from: the one run besides the engine's own (the
    process run, or serve's `unattributed`) that `recorded`'s irimi put exchanges or tool
    calls in - the SDK's, or a `header` run whose start never reached irimi (W10's control
    faults) - or, when there is none, the engine's own: the process run (W9, W11). Read off the
    events it handed its store, so a store that holds other runs too, as a replay's holds its
    recording's, still names `recorded`'s own. A recording with several (W3, W8) has no one run to
    replay, and raises."""
    process = recorded.engine_run_id()
    events = [*recorded.reported, *recorded.handed_events]
    runs = sorted({event.run_id for event in events} - {process})
    if len(runs) > 1:
        raise AssertionError(f"{recorded.scenario} stored {len(runs)} runs; a replay names one")
    return runs[0] if runs else process


def replay_targets(recorded: Result) -> list[str]:
    """Every run a replay of `recorded` can answer from, in the order they started: each run
    its irimi stored besides the process run - an SDK run, a run W9 posted by hand, a `header` run -
    exchanges or none, or the process run alone when there is none (W9's scripts). A process that
    held several runs (W3, W5's cascade, W8) is replayed once per run, each answering that run
    alone. Read before anything replays into its store."""
    process = recorded.process_run_id()
    runs = [r for r in recorded.stored().list_runs() if r.run_id != process]
    runs.sort(key=lambda r: (r.started_at is None, r.started_at or 0.0, r.run_id))
    return [r.run_id for r in runs] or [process]


def counterpart(recorded: Result, replayed: Result, run_id: str) -> str | None:
    """The run of `replayed` that stands for `recorded`'s run `run_id`: the replay's one run,
    which every run its child's SDK started adopted, as its result names it. None when the
    replay stored no run, refused before it ran."""
    assert replayed.replay is not None and replayed.replay.source_run_id == run_id
    return replayed.replay.replay_run_id


def _execute(
    workflow: Workflow,
    scenario_name: str,
    mode: str,
    workdir: Path,
    store: Path | None,
    *,
    home: Path | None = None,
    env: dict[str, str] | None = None,
    replay_of: str | None = None,
    how: _Replay | None = None,
) -> Result:
    """`run` and `replay` alike: `home` is the `IRIMI_HOME` to run in, `workdir`'s own unless a
    replay names the recording's, `env` is laid over the scenario's environment, `replay_of` is the
    stored run a replay answers from, and `how` how it is replayed."""
    scenario = workflow.scenarios[scenario_name]
    workdir.mkdir(parents=True, exist_ok=True)
    state, cwd, tmp = (workdir / d for d in ("state", "cwd", "tmp"))
    home = home if home is not None else workdir / "home"
    for d in (home, state, cwd, tmp):
        d.mkdir(exist_ok=True)
    obs_path = workdir / "obs.jsonl"
    obs_path.write_text("")

    world = World()
    if scenario.setup is not None:
        scenario.setup(world)
    internet = FakeInternet(world.services())
    internet.start()
    irimi_out: list[str] = []
    reported: list[Exchange] = []
    handed: list[Exchange] = []
    handed_events: list[Exchange | ToolCall] = []
    result_file = workdir / "result.json"
    try:
        for host, action, match, times in scenario.faults:
            internet.inject(host, action, match, times)
        agent_env = _agent_env(internet, state, obs_path, home, tmp)
        agent_env.update(scenario.env)
        agent_env.update(env or {})
        cmd = [sys.executable, "-m", workflow.module, *scenario.argv]
        flags = ("--store", str(store)) if store is not None else ()
        if mode == "bare":
            code = _run_bare(cmd, agent_env, cwd, internet)
        elif mode == "shadow":
            argv = ["shadow", "--port", "0", *flags, *scenario.shadow_flags, "--", *cmd]
            with _watching_the_store(reported, handed, handed_events) as build:
                run_irimi: Callable[[], int] = functools.partial(
                    irimi_main, argv, engine_factory=build
                )
                code, irimi_out = _run_irimi(
                    run_irimi, agent_env, cwd, tmp, internet, scenario, workdir
                )
        elif mode == "replay":
            assert replay_of is not None and how is not None
            argv = ["replay", "--port", "0", *flags, "--result-file", str(result_file)]
            # As its user types it: the run id, then `-- <cmd>` for a command.
            argv += [*how.options, replay_of, *(["--", *cmd] if how.command else [])]
            with _watching_the_store(reported, handed, handed_events) as build:
                if how.call is None:
                    run_irimi = functools.partial(irimi_main, argv, engine_factory=build)
                else:
                    run_irimi = _calling_sdk_replay(replay_of, how.call, store, result_file, build)
                code, irimi_out = _run_irimi(
                    run_irimi, agent_env, cwd, tmp, internet, scenario, workdir
                )
        else:
            with _watching_the_store(reported, handed, handed_events) as build:
                code, irimi_out = _run_serve(
                    cmd, agent_env, cwd, tmp, internet, scenario, workdir, build, flags
                )
    finally:
        internet.stop()
    obs = [json.loads(line) for line in obs_path.read_text().splitlines() if line.strip()]
    return Result(
        workflow,
        scenario_name,
        mode,
        code,
        obs,
        irimi_out,
        world,
        internet,
        home,
        state,
        cwd,
        tmp,
        reported,
        handed,
        handed_events,
        store,
        _replay_result(result_file) if mode == "replay" else None,
    )


def _replay_result(path: Path) -> testing.ReplayResult:
    """The result file a replay wrote, as `testing.replay` reads it back."""
    return replay_result_from_json(json.loads(path.read_text(encoding="utf-8")))


def _calling_sdk_replay(
    run_id: str,
    call: tuple[Any, ...],
    store: Path | None,
    result_file: Path,
    build: EngineFactory,
) -> Callable[[], int]:
    """`testing.replay(run_id, fn, args=args, store=store)` for `call` = `(fn, *args)`, or of the
    recorded trigger for an empty `call`, its `irimi` run in this process, building its engine
    with `build`: what it returned written to `result_file`, as `irimi replay --result-file`
    writes it, and its exit code."""

    def run() -> int:
        real = testing._run_irimi
        testing._run_irimi = lambda argv, env: irimi_main(argv, engine_factory=build)
        try:
            if call:
                got = testing.replay(run_id, call[0], args=tuple(call[1:]), store=store)
            else:
                got = testing.replay(run_id, store=store)
        finally:
            testing._run_irimi = real
        result_file.write_text(json.dumps(replay_result_to_json(got)), encoding="utf-8")
        return got.exit_code

    return run


def _agent_env(
    internet: FakeInternet, state: Path, obs_path: Path, home: Path, tmp: Path
) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if k in INHERITED_ENV or k.startswith("LC_")}
    env.update(CANARIES)
    env.update(
        {
            "STRIPE_API_BASE": internet.base("api.stripe.com"),
            "SLACK_API_BASE": internet.base("slack.com"),
            "SLACK_HOOKS_API_BASE": internet.base("hooks.slack.com"),
            "ANTHROPIC_API_BASE": internet.base("api.anthropic.com"),
            "OPENAI_API_BASE": internet.base("api.openai.com"),
            "LANGSMITH_API_BASE": internet.base("api.smith.langchain.com"),
            "WORKFLOW_INTERNET_PORT": str(internet.port),
            "WORKFLOW_STATE": str(state),
            "WORKFLOW_OBS": str(obs_path),
            "WORKFLOW_HTTP_TIMEOUT": "5",
            "PYTHONPATH": str(REPO_ROOT),
            "PYTHONUNBUFFERED": "1",
            "IRIMI_HOME": str(home),
            "TMPDIR": str(tmp),
        }
    )
    return env


def _run_bare(cmd: list[str], env: dict[str, str], cwd: Path, internet: FakeInternet) -> int:
    """The agent as a plain subprocess. Every proxy variable names the fake internet, so a client
    that honours any of them never dials a real address: an `https://` URL gets the fake's refusal
    of `CONNECT`, not a real TLS connection."""
    proxy = f"http://127.0.0.1:{internet.port}"
    env = {**env, **dict.fromkeys(PROXY_VARS, proxy)}
    env["NO_PROXY"] = env["no_proxy"] = runner.NO_PROXY_VALUE  # as `irimi shadow` sets it
    returncode = subprocess.run(cmd, env=env, cwd=cwd, timeout=180, check=False).returncode
    return runner.exit_code_for(returncode)


def _run_irimi(
    run: Callable[[], int],
    env: dict[str, str],
    cwd: Path,
    tmp: Path,
    internet: FakeInternet,
    scenario: Scenario,
    workdir: Path,
) -> tuple[int, list[str]]:
    """`run`, which runs irimi in this process, as a user would run it from `cwd`: `irimi shadow
    -- <agent>`, or `irimi replay <run id>`, or `testing.replay` (`_calling_sdk_replay`)."""
    buf = io.StringIO()
    with _as_irimi(env, cwd, tmp, internet, scenario, workdir), contextlib.redirect_stdout(buf):
        code = run()
    return code, buf.getvalue().splitlines()


@contextlib.contextmanager
def _as_irimi(
    env: dict[str, str],
    cwd: Path,
    tmp: Path,
    internet: FakeInternet,
    scenario: Scenario,
    workdir: Path,
) -> Iterator[None]:
    """The current process made the one irimi runs in, for `irimi shadow` and `irimi serve`
    alike: the agent's environment, `cwd`, TMPDIR, the scenario's maps, the fake resolver,
    and a CA under the run's `IRIMI_HOME`.

    irimi's L3 read is urllib in this process. With no proxy variable set, urllib on macOS falls
    back to the system's proxy settings, which would carry the read (and its canary credential)
    past the fake names to a real proxy. `no_proxy=*` keeps it direct. irimi overwrites both
    spellings for its child, and serve's agent is given its own, so the agent still sees exactly
    irimi's `NO_PROXY`.
    """
    harness_env = {**env, "NO_PROXY": "*", "no_proxy": "*"}
    with (
        environ(harness_env),
        contextlib.chdir(cwd),
        _tempdir(tmp),
        scenario_maps(scenario, workdir),
        internet.resolving(),
    ):
        ca.generate_ca(ca.ca_paths())
        yield


def _run_serve(
    cmd: list[str],
    env: dict[str, str],
    cwd: Path,
    tmp: Path,
    internet: FakeInternet,
    scenario: Scenario,
    workdir: Path,
    build: EngineFactory,
    flags: tuple[str, ...] = (),
) -> tuple[int, list[str]]:
    """`irimi serve` in this process, as `_run_irimi` runs `irimi shadow`, on a thread of
    its own; the agent beside it as its own process; and irimi stopped when the agent exits.

    The command is parsed by irimi's own parser and run by the function `irimi serve`
    runs, with the `stop` event in place of SIGTERM, which would reach this whole test process,
    and `build` building its engine.
    The agent is given exactly the variables an agent container is (README, "Serve mode"): the
    proxy, the CA, `IRIMI_ENGINE_ACTIVE` and `IRIMI_CONTROL`, and never `IRIMI_RUN`, so a
    request no SDK run names is `unattributed`.
    """
    buf = io.StringIO()
    stop = threading.Event()
    codes: list[int] = []
    argv = ["serve", "--port", "0", *flags, *scenario.serve_flags]
    args = parse_args(argv)
    before = set(threading.enumerate())
    with _as_irimi(env, cwd, tmp, internet, scenario, workdir), contextlib.redirect_stdout(buf):
        serving = threading.Thread(
            target=lambda: codes.append(args.func(args, stop, engine_factory=build)),
            name="irimi-serve",
        )
        serving.start()
        try:
            port = _served_port(buf, serving)
            if port is None:
                # Refused before it listened, as `irimi shadow` refuses before it starts its
                # child: no agent is started beside a proxy that is not there, and serve's own
                # exit is the run's (W8 `map_refused`).
                serving.join(timeout=SERVE_TIMEOUT_S)
                if serving.is_alive() or codes in ([], [0]):
                    raise AssertionError(f"irimi serve printed no banner: {codes}")
                return codes[0], buf.getvalue().splitlines()
            agent_env = _serve_env(env, port, ca.ca_paths().cert)
            done = subprocess.run(cmd, env=agent_env, cwd=cwd, timeout=180, check=False)
            code = runner.exit_code_for(done.returncode)
        finally:
            stop.set()
            serving.join(timeout=SERVE_TIMEOUT_S)
    if serving.is_alive() or codes != [0]:
        raise AssertionError(f"irimi serve did not stop cleanly: {codes}")
    # Stopped means stopped: the engine's thread joined and the store closed, its writer thread
    # with it, by the time serve returned. A thread of the engine's event loop's executor,
    # told to shut down as the loop closed, may take a moment to see it, so each is given that
    # long to end; the fake internet's own handlers may still be finishing an answer.
    started = [t for t in threading.enumerate() if t not in before and FAKE_HANDLER not in t.name]
    deadline = time.monotonic() + THREAD_EXIT_S
    for thread in started:
        thread.join(max(0.0, deadline - time.monotonic()))
    left = [t.name for t in started if t.is_alive()]
    if left:
        raise AssertionError(f"irimi serve left threads running: {left}")
    return code, buf.getvalue().splitlines()


def _served_port(buf: io.StringIO, serving: threading.Thread) -> int | None:
    """The port in serve's banner, once it has printed it, or None if serve ended without one."""
    deadline = time.monotonic() + SERVE_TIMEOUT_S
    while time.monotonic() < deadline:
        # Alive is read first, so a serve that printed its banner and then ended is not missed.
        alive = serving.is_alive()
        match = re.search(r" · listening on \S+:(\d+) · ", buf.getvalue())
        if match:
            return int(match.group(1))
        if not alive:
            return None
        time.sleep(0.01)
    raise AssertionError(f"irimi serve printed no banner: {buf.getvalue()!r}")


def _serve_env(env: dict[str, str], port: int, cert: Path) -> dict[str, str]:
    """The agent's environment beside a serve: the README's serve-mode variables, naming irimi as
    the agent connects to it, here loopback."""
    listener = f"http://127.0.0.1:{port}"
    return {
        **env,
        **dict.fromkeys(PROXY_ENVS, listener),
        "NO_PROXY": runner.NO_PROXY_VALUE,
        "no_proxy": runner.NO_PROXY_VALUE,
        "SSL_CERT_FILE": str(cert),
        "REQUESTS_CA_BUNDLE": str(cert),
        ENGINE_ACTIVE_ENV: "1",
        CONTROL_ENV: f"{listener}/_irimi",
    }


@contextlib.contextmanager
def _watching_the_store(
    reported: list[Exchange], handed: list[Exchange], handed_events: list[Exchange | ToolCall]
) -> Iterator[EngineFactory]:
    """`irimi shadow` unchanged, but each exchange it prints a line for is appended to `reported`,
    and each one its engine hands the trace store to `handed`, and to `handed_events` with each
    tool call the control endpoint hands it. The block is given the engine factory to pass irimi
    (`cli.main(engine_factory=)`): `cli.compose.build_engine`, handed an `on_exchange` that
    records each exchange before it prints its line. `cli.compose` looks `DirectoryStore` up when
    it opens the store, so swapping that name is enough, as `tests/e2e/test_trace_roundtrip.py`
    does. Not `report.terminal.exchange_line` itself, which `irimi replay` also calls for the line
    of its divergence point."""

    def building(
        run: compose.Run,
        on_exchange: OnExchange,
        on_tool_call: OnToolCall | None = None,
        on_tls_failure: OnTlsFailure | None = None,
    ) -> Engine:
        def printing(ex: Exchange) -> None:
            reported.append(ex)
            on_exchange(ex)

        return compose.build_engine(run, printing, on_tool_call, on_tls_failure)

    class Handed(store_writer.DirectoryStore):
        def record(self, exchange: Exchange) -> None:
            handed.append(exchange)
            handed_events.append(exchange)
            super().record(exchange)

        def record_tool_call(self, call: ToolCall) -> None:
            handed_events.append(call)
            super().record_tool_call(call)

    with mock.patch.object(store_writer, "DirectoryStore", Handed):
        yield building


@contextlib.contextmanager
def _tempdir(path: Path) -> Iterator[None]:
    """`tempfile`'s directory in this process: it reads TMPDIR once and caches it."""
    saved = tempfile.tempdir
    tempfile.tempdir = str(path)
    try:
        yield
    finally:
        tempfile.tempdir = saved
