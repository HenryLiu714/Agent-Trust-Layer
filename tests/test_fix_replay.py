"""`irimi compare` over two stored writes that print one summary line and differ in what they
sent: the field diff under the pair names each differing member so that no two print alike."""

import dataclasses
import json
import unicodedata

import pytest

from irimi.cli.compose import maps_for_stored_runs
from irimi.core.exchange import Exchange, Request, Response
from irimi.protocol.trace import RunRecord, StoredRun
from irimi.replay import compare


def _posted(run_id: str, body: dict[str, object]) -> StoredRun:
    request = Request(
        "POST",
        "https",
        "slack.com",
        443,
        "/api/chat.postMessage",
        "",
        (("content-type", "application/json"),),
        json.dumps(body).encode(),
    )
    post = Exchange(
        request,
        Response(200, (), b'{"ok":true}'),
        "slack",
        "chat.postMessage",
        "write",
        "fake-L1",
        run_id,
        started_at=1.0,
        ended_at=2.0,
    )
    return StoredRun(RunRecord.header(run_id), [post])


@pytest.mark.parametrize(
    ("one", "other"),
    [
        ("a\nb", "a\\nb"),  # a newline and a typed one
        ("note", "note\u200b"),  # a zero-width space prints as nothing
        ("a b", "a\u00a0b"),  # a space and a no-break one
    ],
)
def test_members_that_print_alike_are_named_apart_in_a_field_diff(one, other):
    a = _posted("a", {"channel": "C1", "text": "hi", "metadata": {one: 1}})
    b = _posted("b", {"channel": "C1", "text": "hi", "metadata": {other: 1}})
    comparison = compare.compare_runs(a, b, maps_for_stored_runs())
    assert comparison.exit_code == 1
    rows = comparison.lines()
    assert rows[0].startswith('  ~ A ○ slack  post to #C1: "hi"')
    diff = [line.strip() for line in rows[2:] if line.startswith("      ")]
    # As a reader sees each name: what prints as nothing dropped, what prints as a space a space.
    names = {_seen(line.split(": ", 1)[0]) for line in diff}
    assert len(diff) == 2 and len(names) == 2, diff


def _seen(text: str) -> str:
    kept = (char for char in text if unicodedata.category(char) != "Cf")
    return "".join(" " if unicodedata.category(char) == "Zs" else char for char in kept)


def test_a_plain_member_is_named_as_it_is():
    a = _posted("a", {"channel": "C1", "text": "hi", "metadata": {"source": "web"}})
    b = _posted("b", {"channel": "C1", "text": "hi", "metadata": {"source": "cli"}})
    rows = compare.compare_runs(a, b, maps_for_stored_runs()).lines()
    assert rows[2] == '      body.metadata.source: "web" -> "cli"'


def test_a_quoted_member_names_its_escapes():
    a = _posted("a", {"channel": "C1", "text": "hi", "metadata": {"a​": 1}})
    b = _posted("b", {"channel": "C1", "text": "hi", "metadata": {"a​": 2}})
    rows = compare.compare_runs(a, b, maps_for_stored_runs()).lines()
    assert rows[2] == '      body.metadata."a\\u200b": 1 -> 2'


def _posts(run_id: str, *texts: str) -> StoredRun:
    events = [
        event
        for i, text in enumerate(texts)
        for event in _posted(f"{run_id}{i}", {"channel": "C1", "text": text}).events
    ]
    events = [dataclasses.replace(event, run_id=run_id) for event in events]
    return StoredRun(RunRecord.header(run_id), events)


def test_a_post_only_one_run_made_never_prints_like_one_both_made():
    """B posts a typed `\\n` and then the newline A posted: the newline lines up `=`, and the typed
    one, `+`, is escaped so that it does not read as the same post made twice."""
    a = _posts("a", "a\nb")
    b = _posts("b", "a\\nb", "a\nb")
    rows = compare.compare_runs(a, b, maps_for_stored_runs()).lines()
    assert rows[:2] == [
        '  + ○ slack  post to #C1: "a\\\\nb"  faked; not checked',
        '  = ○ slack  post to #C1: "a\\nb"  faked; not checked',
    ]
