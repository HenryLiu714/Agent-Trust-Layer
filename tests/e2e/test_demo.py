"""`scripts/demo.py`, what `make demo` runs: each example under `irimi shadow` against the fake
internet, with no keys, and no write reaching a fake service."""

import pytest

from tests.support.scenarios import demo

pytestmark = pytest.mark.e2e


def test_the_hello_agent_reads_for_real_and_its_two_writes_are_faked(home, tmp_path, capfd):
    code, internet = demo().run("hello", tmp_path)
    out = capfd.readouterr().out
    assert code == 0, out
    assert internet.writes() == []
    assert [r.host for r in internet.requests() if r.method == "GET"] == [
        "api.github.com",
        "api.stripe.com",  # irimi's check read of the charge, before it faked the refund
    ]
    assert "read   200 answered by the service" in out
    assert "refund 200 answered by fake-L1" in out
    assert "slack  200 answered by fake-L0" in out
    assert (
        "  ○ stripe  refund ? on ch_hello  faked; checked against live state: would succeed" in out
    )
    assert '  ○ slack  post via webhook: "refunded ch_hello"  faked; not checked' in out


def test_the_replay_example_records_a_run_with_its_tools_and_its_model_call(home, tmp_path, capfd):
    code, internet = demo().run("replay", tmp_path)
    out = capfd.readouterr().out
    assert code == 0, out
    assert internet.writes() == []
    assert "Refunded 4900." in out.splitlines()
    assert "  ○ tool  db.record_decision  fake ran instead" in out
    # The model call is the SDK run's, so a replay of it answers it from the recording.
    [run] = [line for line in out.splitlines() if line.startswith("  run ")]
    assert "examples.replay.agent:handle_ticket(...)  4 exchanges · 1 write" in run, out


def test_the_demo_refuses_an_example_it_does_not_know(capsys):
    assert demo().main(["nonesuch"]) == 2
    assert (
        "usage: python scripts/demo.py [hello | quickstart | replay | w01]"
        in capsys.readouterr().err
    )
