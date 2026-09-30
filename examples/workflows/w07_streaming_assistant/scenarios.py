"""W7's scenarios: one question each; the script answers from the question alone."""

from __future__ import annotations

import json
import threading
from typing import Any

from examples.workflows.harness.internet import Resp
from examples.workflows.harness.run import Scenario, Workflow
from examples.workflows.harness.services import (
    LlmCall,
    LlmTurn,
    ScriptedLLM,
    World,
    fake_langsmith,
)

CHANNEL = "C0CHAT"
# Shaped like a live Stripe key, so the redaction rules (#69) would recognise it on disk.
SECRET = "sk_live_CANARYstreamsecret0000000000"
ANSWER = (
    "Refunds return money to the original payment method. A partial refund leaves the rest "
    "of the charge in place, and a charge can be refunded until nothing is left."
)
# `secret_split_across_writes`: an answer of some 16 KB, the secret whole in the middle of it.
_RULES = " ".join(f"Rule {i}: a refund goes back to the card it came from." for i in range(300))
LONG_ANSWER = f"{_RULES[:8000]} The test key is {SECRET}. {_RULES[8000:]}"
# How many characters of text each `text_delta` of that answer carries.
LONG_DELTA = 256


def chat_script(call: LlmCall) -> LlmTurn:
    question = call.last_user_text()
    if call.provider == "anthropic":
        if "key" in question:
            return LlmTurn(text=f"The test key is {SECRET}. Keep it out of logs.")
        return LlmTurn(text=ANSWER)
    # OpenAI: the model that decides whether to post. It sees the Anthropic answer.
    if "summary" in question.lower():
        return LlmTurn(tool_calls=(("post_summary", {"channel": CHANNEL, "text": question}),))
    return LlmTurn(text="No summary needed.")


def summarize_script(call: LlmCall) -> LlmTurn:
    if call.provider == "anthropic":
        return LlmTurn(text="Summary: refunds go back to the card within 5-10 days.")
    return chat_script(call)


def long_script(call: LlmCall) -> LlmTurn:
    if call.provider == "anthropic":
        return LlmTurn(text=LONG_ANSWER)
    return chat_script(call)


def pair_answer(caller: str) -> str:
    return f"For caller {caller}: {ANSWER}"


def pair_decision(caller: str) -> str:
    return f"No summary needed for caller {caller}."


def pair_script(call: LlmCall) -> LlmTurn:
    """`two_callers`: each chat's answers name its caller, so a stream recorded against the other
    caller's request would show."""
    caller = "1" if "caller 1" in call.last_user_text() else "2"
    if call.provider == "anthropic":
        return LlmTurn(text=pair_answer(caller))
    return LlmTurn(text=pair_decision(caller))


def _sse(doc: dict[str, Any]) -> bytes:
    return f"data: {json.dumps(doc)}\n\n".encode()


class SplittingLLM(ScriptedLLM):
    """Streams an OpenAI tool call the way OpenAI does: the id and name in one delta, then the
    arguments a few characters per delta. The shared fake sends a whole call in one delta."""

    def _openai(self, body: dict[str, Any]) -> Resp:
        resp = super()._openai(body)
        if resp.chunks is None:
            return resp
        out = []
        for chunk in resp.chunks:
            text = chunk.decode()
            doc = json.loads(text[6:]) if text.startswith("data: {") else {}
            calls = doc.get("choices", [{}])[0].get("delta", {}).get("tool_calls")
            if not calls:
                out.append(chunk)
                continue
            call = calls[0]
            head = {**call, "function": {"name": call["function"]["name"], "arguments": ""}}
            out.append(_sse({"choices": [{"index": 0, "delta": {"tool_calls": [head]}}]}))
            args = call["function"]["arguments"]
            for i in range(0, len(args), 5):
                piece = {"index": call["index"], "function": {"arguments": args[i : i + 5]}}
                out.append(_sse({"choices": [{"index": 0, "delta": {"tool_calls": [piece]}}]}))
        resp.chunks = out
        return resp


def _event(name: str, doc: dict[str, Any]) -> bytes:
    return f"event: {name}\ndata: {json.dumps(doc)}\n\n".encode()


def _mid_data_line(event: bytes) -> int:
    """An offset strictly inside `event`'s `data:` line: the middle of its secret if it holds it,
    else the middle of the line."""
    if SECRET.encode() in event:
        return event.index(SECRET.encode()) + len(SECRET) // 2
    start = event.index(b"\ndata: ") + 1
    return (start + event.index(b"\n", start)) // 2


class MidLineWritesLLM(ScriptedLLM):
    """Streams Anthropic's text LONG_DELTA characters per `text_delta`, with the secret whole in
    one of them, and writes the stream in pieces that each end in the middle of a `data:` line:
    every event is cut inside that line and its second half goes out with the next event's first.
    The event that holds the secret is cut in the middle of the secret. So each boundary between
    two writes falls mid-line, and one falls mid-secret (#71): redaction has to read the chunks
    joined, and the recorded chunk lengths have to follow the redacted line."""

    def _anthropic(self, body: dict[str, Any]) -> Resp:
        resp = super()._anthropic(body)
        if resp.chunks is None:
            return resp
        events = [chunk for chunk in resp.chunks if b'"text_delta"' not in chunk]
        text = "".join(
            json.loads(chunk.decode().split("\ndata: ", 1)[1])["delta"]["text"]
            for chunk in resp.chunks
            if b'"text_delta"' in chunk
        )
        at = text.index(SECRET)
        cuts = [
            i for i in range(LONG_DELTA, len(text), LONG_DELTA) if not at < i < at + len(SECRET)
        ]
        deltas = [
            _event(
                "content_block_delta",
                {
                    "type": "content_block_delta",
                    "index": 0,
                    "delta": {"type": "text_delta", "text": text[a:b]},
                },
            )
            for a, b in zip([0, *cuts], [*cuts, len(text)], strict=True)
        ]
        # message_start, content_block_start, the deltas, then the rest in order.
        events = [*events[:2], *deltas, *events[2:]]
        writes, carry = [], b""
        for event in events:
            cut = _mid_data_line(event)
            writes.append(carry + event[:cut])
            carry = event[cut:]
        resp.chunks = [*writes, carry]
        return resp


class LockstepLLM(ScriptedLLM):
    """Holds each provider's two calls until both have arrived, so the two callers' streams are
    written at once and irimi copies two flows' chunks interleaved (#71)."""

    def __init__(self, script: Any) -> None:
        super().__init__(script)
        self._together = {
            name: threading.Barrier(2, timeout=10) for name in ("anthropic", "openai")
        }

    def _anthropic(self, body: dict[str, Any]) -> Resp:
        self._together["anthropic"].wait()
        return super()._anthropic(body)

    def _openai(self, body: dict[str, Any]) -> Resp:
        self._together["openai"].wait()
        return super()._openai(body)


def _world(world: World) -> None:
    world.llm.script = chat_script


def _traced(world: World) -> None:
    _world(world)
    world.extra.append(fake_langsmith())


def _splitting(world: World) -> None:
    world.llm = SplittingLLM(summarize_script)
    world.slack.add_channel(CHANNEL, "payments-team")


def _mid_line(world: World) -> None:
    world.llm = MidLineWritesLLM(long_script)


def _lockstep(world: World) -> None:
    world.llm = LockstepLLM(pair_script)


QUESTION = "How do refunds work?"

WORKFLOW = Workflow(
    name="w07_streaming_assistant",
    summary="A chat endpoint streaming back to its caller: an embeddings call, an Anthropic SSE "
    "stream, an OpenAI SSE stream whose tool call becomes a Slack post.",
    scenarios={
        "normal": Scenario(
            (QUESTION, "read_all"),
            env={"LANGSMITH_TRACING": "true"},
            setup=_traced,
            doc="two streams, no post, and one LangSmith trace: telemetry (#70)",
        ),
        "secret_in_stream": Scenario(
            ("What is the test key?", "read_all"),
            setup=_world,
            doc="a key-shaped secret inside the stream reaches the caller intact",
        ),
        "caller_disconnects": Scenario(
            (QUESTION, "hang_up"),
            setup=_world,
            doc="the caller resets after the first event; the run ends in error",
        ),
        "upstream_reset": Scenario(
            (QUESTION, "read_all"),
            setup=_world,
            faults=(
                ("api.anthropic.com", "reset-stream", lambda req: req.path == "/v1/messages", 1),
            ),
            doc="Anthropic resets after the first event of its stream",
        ),
        "reset_before_stream": Scenario(
            (QUESTION, "read_all"),
            setup=_world,
            faults=(("api.anthropic.com", "reset", lambda req: req.path == "/v1/messages", 1),),
            doc="Anthropic resets before its stream opens",
        ),
        "tool_call_across_deltas": Scenario(
            ("Please post a summary of refunds", "read_all"),
            setup=_splitting,
            doc="a tool call streamed in pieces becomes a Slack post",
        ),
        "secret_split_across_writes": Scenario(
            ("What is the test key?", "read_all"),
            setup=_mid_line,
            doc="a 16 KB stream written in pieces that end mid-line, one of them mid-secret",
        ),
        "two_callers": Scenario(
            (QUESTION, "two_callers"),
            setup=_lockstep,
            doc="two callers at once: two chats' streams through the proxy side by side",
        ),
    },
)
