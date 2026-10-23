"""Asking a policy for its decision, the way the session asks, and flattening the answer
into the fields tests assert on."""

import json
from dataclasses import dataclass, replace

from irimi import servicemaps
from irimi.core.exchange import Request, Response
from irimi.decide import reader, targets
from irimi.decide.policy import Decided, Delegate, Respond, ShadowPolicy
from irimi.servicemaps.classify import classify
from tests.support.builders import FORM_CONTENT_TYPE
from tests.support.credentials import STRIPE_TEST_X

SHIPPED = servicemaps.MapIndex(tuple(servicemaps.loader.load_shipped()))


def make_request(
    method: str = "GET",
    host: str = "api.stripe.com",
    path: str = "/v1/charges",
    body: bytes = b"",
    content_type: str | None = None,
) -> Request:
    """A request to Stripe, by default a read of its charges, with no header but a content type."""
    headers = (("content-type", content_type),) if content_type is not None else ()
    return Request(
        method=method,
        scheme="https",
        host=host,
        port=443,
        path=path,
        query="",
        headers=headers,
        body=body,
    )


@dataclass(frozen=True)
class Flat:
    """A policy's `Decided`, flattened into the fields these tests assert on: who answers and
    with what, and what irimi decided about the write."""

    answered_by: str
    response: Response | None
    flags: tuple
    issued: tuple
    forward_to: targets.ForwardTo | None = None
    precondition: str | None = None
    rejection_code: str = ""
    currency: str = ""
    would_fire: tuple = ()
    overlay: str | None = None


def flat(decided: Decided) -> Flat:
    """The decision `decided` holds, as a `Flat`."""
    d, flags, issued = decided.decision, decided.flags, decided.issued
    if isinstance(d, Respond):
        p = d.outcome.precondition
        return Flat(
            d.answered_by,
            d.response,
            flags,
            issued,
            None,
            p.outcome,
            p.rejection_code,
            p.currency,
            d.outcome.would_fire,
            d.overlay,
        )
    if isinstance(d, Delegate):
        p = d.precondition
        return Flat(
            d.answered_by, None, flags, issued, d.target, p.outcome, p.rejection_code, p.currency
        )
    return Flat(d.answered_by, None, flags, issued)


def answer(policy, request, classification, write_log=(), run_id=""):
    """`policy`'s decision, flattened, with the answer target resolved first, as the session
    resolves it."""
    target = targets.delegate(request, classification)
    return flat(policy.decide(request, classification, write_log, run_id, target))


def answer_shipped(request: Request):
    """Classify against the shipped maps, then answer - the same order the addon uses."""
    return answer(ShadowPolicy(), request, classify(request, SHIPPED))


def shipped_with_targets(targets=(), target_reads=()):
    """The shipped maps with `--target` and `--target-reads` flags applied."""
    maps = servicemaps.loader.apply_cli_targets(
        list(servicemaps.loader.load_shipped()), targets, target_reads
    )
    return servicemaps.MapIndex(tuple(maps))


def answer_with(index, request):
    """`ShadowPolicy`'s answer to `request` classified against `index`."""
    return answer(ShadowPolicy(), request, classify(request, index))


class CannedReader(reader.Reader):
    """A reader serving canned answers in order, and writing down every request it was asked.

    An answer that is an exception is raised instead of returned, so "the reader raised" is one
    more canned answer rather than a second helper.
    """

    def __init__(self, answers):
        self.answers = list(answers)
        self.asked: list[Request] = []

    def __call__(self, request):
        self.asked.append(request)
        answer = self.answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer


def canned_reader(*answers) -> CannedReader:
    """A `CannedReader` serving `answers` in order."""
    return CannedReader(answers)


def json_response(status: int, document) -> Response:
    """A JSON response of `document` with `status`."""
    return Response(status, (("content-type", "application/json"),), json.dumps(document).encode())


def charge_document(ident="ch_REAL", amount=4900, refunded_so_far=0):
    """A Stripe charge of `amount` with `refunded_so_far` refunded."""
    return {
        "id": ident,
        "object": "charge",
        "amount": amount,
        "amount_refunded": refunded_so_far,
        "refunded": refunded_so_far == amount,
        "currency": "usd",
    }


AGENT_HEADERS = (
    ("authorization", f"Bearer {STRIPE_TEST_X}"),
    ("stripe-account", "acct_1"),
    ("stripe-version", "2024-06-20"),
    ("idempotency-key", "idem-1"),
    ("user-agent", "stripe-python"),
    ("content-type", FORM_CONTENT_TYPE),
)


def refund_request(body: bytes = b"charge=ch_REAL&amount=4900") -> Request:
    """A refund POST carrying the headers stripe-python sends."""
    return replace(make_request("POST", path="/v1/refunds", body=body), headers=AGENT_HEADERS)


def checked(reader, request, index=SHIPPED, write_log=()):
    """Answer `request` the way the shadow run will: a real reader, the run's maps, its log."""
    policy = ShadowPolicy(reader=reader, maps=index)
    return answer(policy, request, classify(request, index), write_log, "t3st")


def slack_post_request(channel: str) -> Request:
    """A `chat.postMessage` to `channel` as slack_sdk posts it."""
    return make_request(
        "POST",
        host="slack.com",
        path="/api/chat.postMessage",
        body=json.dumps({"channel": channel, "text": "hi"}).encode(),
        content_type="application/json",
    )


def ask(policy: ShadowPolicy, request: Request):
    """`_checked`, but against a policy the test holds, so two calls share one run's store."""
    return answer(policy, request, classify(request, SHIPPED), (), "t3st")
