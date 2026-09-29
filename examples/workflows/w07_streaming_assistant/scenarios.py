"""W7's scenarios: one question each; the script answers from the question alone."""

from __future__ import annotations

import json
from typing import Any

from examples.workflows.harness.internet import Resp
from examples.workflows.harness.run import Scenario, Workflow
from examples.workflows.harness.services import LlmCall, LlmTurn, ScriptedLLM, World

CHANNEL = "C0CHAT"
# Shaped like a live Stripe key, so the redaction rules (#69) would recognise it on disk.
SECRET = "sk_live_CANARYstreamsecret0000000000"
ANSWER = (
    "Refunds return money to the original payment method. A partial refund leaves the rest "
    "of the charge in place, and a charge can be refunded until nothing is left."
)


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


def _world(world: World) -> None:
    world.llm.script = chat_script


def _splitting(world: World) -> None:
    world.llm = SplittingLLM(summarize_script)
    world.slack.add_channel(CHANNEL, "payments-team")


QUESTION = "How do refunds work?"

WORKFLOW = Workflow(
    name="w07_streaming_assistant",
    summary="A chat endpoint streaming back to its caller: an embeddings call, an Anthropic SSE "
    "stream, an OpenAI SSE stream whose tool call becomes a Slack post.",
    scenarios={
        "normal": Scenario((QUESTION, "read_all"), setup=_world, doc="two streams, no post"),
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
    },
)
