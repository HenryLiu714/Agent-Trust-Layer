"""Stripe's L2 effects: what a faked write does to a later live read (#43).

The design's v0.1 table, by the write that causes each effect:

    refunds.create          a new refund object; on the charge, `amount_refunded` and `refunded`,
                            and the expanded `refunds` list; on page 1 of the refunds list
    customers.update        the posted fields, on the customer
    payment_intents.cancel  `status`, `canceled_at`, `cancellation_reason`

Two reads beyond the table's letter, both so that irimi does not contradict itself (#43):
`charges.list` gets the same charge effect as `charges.retrieve`, because the refund agent finds its
charge in the list and a list that ignored the refund would disagree with the retrieve; and
`refunds.retrieve` of a minted id is flagged `partial`, because the honest answer is Stripe's 404
and this seam carries a body but no status (#52).

Two rules run through all of it:

* **Never invent a field the live object does not have.** A field Stripe's own response omits is
  one the agent can never see in production, and putting one there hands it a body no live read
  can produce. This is `echo._reflect_over`'s rule, applied to reads.
* **Say `partial` rather than half-apply.** A page the table does not model, a filter it cannot
  read, a refund it minted being fetched by id: the document is left exactly as Stripe sent it
  and the exchange records that the world irimi showed is incomplete.

Pure functions over plain data: no clock, no minting, no I/O, no module state. Phase 5 replay
runs these same functions over a recording.
"""

from collections.abc import Sequence
from typing import Any
from urllib.parse import parse_qsl, urlencode

from irimi.exchange import Request
from irimi.pipeline import REWROTE_HEADER
from irimi.services.model import (
    NOT_EVALUABLE,
    Applied,
    Check,
    NotEvaluable,
    Probe,
    Proposal,
    Read,
    Rejection,
    Rewritten,
    Write,
)

SERVICE = "stripe"
# Stripe's own default page size, and what a list read gets when it names no `limit`.
DEFAULT_LIST_LIMIT = 10
MAX_LIST_LIMIT = 100
# Fields of a live object that are the service's answer and never a caller's argument. The same
# set `echo.SERVICE_OWNED` keeps for the write side; a customer update may not move any of them.
SERVICE_OWNED = frozenset({"id", "object", "created", "livemode"})
# The `GET /v1/refunds` parameters the effects understand. Anything else means irimi cannot say
# where the minted refund belongs on this page, and the read is `partial`. `expand` arrives as
# `expand[0]`, so it is matched by prefix.
KNOWN_LIST_PARAMS = frozenset({"limit", "charge", "payment_intent"})
CURSOR_PARAMS = frozenset({"starting_after", "ending_before"})


def apply_read(read: Read, document: Any, writes: Sequence[Write]) -> Applied:
    """The run's faked Stripe writes, applied to one live read's parsed body."""
    # `read.posted` is ignored, not forgotten: a Stripe read is a GET and posts nothing (#44).
    operation = read.operation
    if not isinstance(document, dict):
        return Applied(document)
    if operation == "charges.retrieve":
        return _charge(document, writes)
    if operation == "charges.list":
        return _charges_list(document, writes)
    if operation == "refunds.list":
        return _refunds_list(read.request, document, writes)
    if operation == "refunds.retrieve":
        return _refund_retrieve(read.request, document, writes)
    if operation == "customers.retrieve":
        return _customer(document, writes)
    if operation == "payment_intents.retrieve":
        return _payment_intent(document, writes)
    return Applied(document)


def rewrite_query(read: Read, writes: Sequence[Write]) -> Rewritten | None:
    """Translate a cursor naming a refund irimi minted, before the read is forwarded.

    Stripe has never heard of that id and answers the page with an error. The minted refund is
    always the newest, so it sits first on page 1 and "everything after it" is the real list from
    its top - which Stripe spells as no cursor at all. So the pair is removed rather than
    substituted, and `Rewritten.removed` names it so the response side knows this page follows
    the minted refund (see `_refunds_list`).
    """
    if read.operation != "refunds.list":
        return None
    minted = {w.answer["id"] for w in _minted_refunds(writes)}
    params = parse_qsl(read.request.query, keep_blank_values=True)
    dropped = [(k, v) for k, v in params if k == "starting_after" and v in minted]
    if not dropped:
        return None
    kept = [(k, v) for k, v in params if (k, v) not in dropped]
    return Rewritten(query=urlencode(kept), removed=f"{dropped[0][0]}={dropped[0][1]}")


# ------------------------------------------------------------------------------- the effects


def _charge(charge: dict[str, Any], writes: Sequence[Write]) -> Applied:
    """`amount_refunded`, `refunded`, and the expanded `refunds` list, on one charge object."""
    mine = [r for r in _minted_refunds(writes) if r.answer.get("charge") == charge.get("id")]
    if not mine or "amount_refunded" not in charge:
        return Applied(charge)
    refunded = _int(charge.get("amount_refunded")) + sum(_int(r.answer.get("amount")) for r in mine)
    charge["amount_refunded"] = refunded
    if "refunded" in charge:
        charge["refunded"] = refunded == charge.get("amount")
    refunds = charge.get("refunds")
    # Stripe sends `refunds` only when the request expanded it, so its presence IS the question
    # "did this caller ask for the list?" - asked of the body rather than of `expand[0]`, which
    # is the same answer with no query parsing.
    if isinstance(refunds, dict) and isinstance(refunds.get("data"), list):
        refunds["data"] = [r.answer for r in reversed(mine)] + refunds["data"]
        if isinstance(refunds.get("total_count"), int):
            refunds["total_count"] += len(mine)
    return Applied(charge, changed=True)


def _charges_list(document: dict[str, Any], writes: Sequence[Write]) -> Applied:
    """The same charge effect, wherever the charge appears. `examples/refund_agent/agent.py`
    picks its charge out of this list by reading `refunded` and `amount_refunded`."""
    data = document.get("data")
    if not isinstance(data, list):
        return Applied(document)
    changed = False
    for item in data:
        if isinstance(item, dict) and _charge(item, writes).changed:
            changed = True
    return Applied(document, changed=changed)


def _refunds_list(request: Request, document: dict[str, Any], writes: Sequence[Write]) -> Applied:
    """Page 1 of `GET /v1/refunds`: the minted refund first, the page kept to its own limit."""
    minted = _minted_refunds(writes)
    if not minted:
        return Applied(document)
    params = parse_qsl(request.query, keep_blank_values=True)
    if any(k in CURSOR_PARAMS for k, _ in params):
        # Some page other than the first, and which real items it holds depends on where the
        # minted refund landed. The v0.1 table models page 1 only.
        return Applied(document, partial=True)
    removed = _header(request, REWROTE_HEADER)
    if removed.startswith("starting_after="):
        # `rewrite_query` removed a cursor naming a refund irimi minted, so this IS the page that
        # follows it: the minted refund belongs on the page before this one, and prepending it
        # here would hand the agent its own refund twice. Fully modeled, nothing to change -
        # unless an OLDER minted refund also belongs after the cursor, which the real list from
        # its top leaves out. That page is not modeled, and saying `full` would hide it (#43).
        ids = [r.answer["id"] for r in minted]
        after = removed.partition("=")[2]
        older = minted[: ids.index(after)] if after in ids else []
        # `is not False` because a refund whose filter cannot be answered MIGHT belong on this
        # page, and "might be missing" is exactly what `partial` says.
        if any(_passes_filters(r.answer, params) is not False for r in older):
            return Applied(document, partial=True)
        return Applied(document)
    if any(not _known_list_param(k) for k, _ in params):
        return Applied(document, partial=True)
    verdicts = [(r, _passes_filters(r.answer, params)) for r in minted]
    if any(passes is None for _, passes in verdicts):
        # One of the run's refunds may or may not belong on this page and irimi cannot tell, so
        # the page it shows may be missing a refund the agent itself created. Say so.
        return Applied(document, partial=True)
    mine = [r for r, passes in verdicts if passes]
    if not mine:
        return Applied(document)
    data = document.get("data")
    if not isinstance(data, list):
        return Applied(document, partial=True)
    page = [r.answer for r in reversed(mine)] + data
    limit = _limit(params)
    if len(page) > limit:
        page = page[:limit]
        document["has_more"] = True
    document["data"] = page
    return Applied(document, changed=True)


def _refund_retrieve(
    request: Request, document: dict[str, Any], writes: Sequence[Write]
) -> Applied:
    """A refund irimi minted is not at Stripe, so this read is the 404 the table does not model.

    Answering it from the write log needs the seam to carry a status as well as a body, which is
    #52. Flagging it is what keeps the gap visible instead of silent.
    """
    ident = request.path.rsplit("/", 1)[-1]
    if any(r.answer["id"] == ident for r in _minted_refunds(writes)):
        return Applied(document, partial=True)
    return Applied(document)


def _customer(customer: dict[str, Any], writes: Sequence[Write]) -> Applied:
    """The posted fields of every `customers.update` for this customer."""
    changed = False
    for write in writes:
        if write.operation != "customers.update" or write.answer.get("id") != customer.get("id"):
            continue
        for name, value in write.posted.items():
            # `name not in customer` is `echo._reflect_over`'s rule: a field the live object does
            # not have is one the real service would not have returned. It also keeps `expand[0]`
            # and friends out, without a second list of parameter names to maintain.
            if name in SERVICE_OWNED or name not in customer:
                continue
            if name == "metadata" and isinstance(value, dict) and isinstance(customer[name], dict):
                value = {**customer[name], **value}  # Stripe merges metadata
            # `changed` only when a field really moved: an update whose every field was skipped
            # leaves the live body as Stripe sent it, and must not be stamped `overlay` (#43).
            if customer[name] != value:
                customer[name] = value
                changed = True
    return Applied(customer, changed=changed)


def _payment_intent(intent: dict[str, Any], writes: Sequence[Write]) -> Applied:
    """`status`, `canceled_at` and `cancellation_reason` for a cancelled payment intent."""
    changed = False
    for write in writes:
        if write.operation != "payment_intents.cancel" or write.answer.get("id") != intent.get(
            "id"
        ):
            continue
        intent["status"] = "canceled"
        if "canceled_at" in intent and isinstance(write.answer.get("created"), int):
            intent["canceled_at"] = write.answer["created"]
        if "cancellation_reason" in intent:
            reason = write.posted.get("cancellation_reason")
            intent["cancellation_reason"] = reason if isinstance(reason, str) else None
        changed = True
    return Applied(intent, changed=changed)


# ------------------------------------------------------------------------------------ helpers


def _minted_refunds(writes: Sequence[Write]) -> list[Write]:
    return [
        w for w in writes if w.operation == "refunds.create" and isinstance(w.answer.get("id"), str)
    ]


def _int(value: Any) -> int:
    """`value` as a whole number of minor units, or 0. `bool` is an `int` and is not one here."""
    return value if isinstance(value, int) and not isinstance(value, bool) else 0


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _header(request: Request, name: str) -> str:
    return next((v for k, v in request.headers if k == name), "")


def _known_list_param(name: str) -> bool:
    return name in KNOWN_LIST_PARAMS or name == "expand" or name.startswith("expand[")


def _passes_filters(refund: dict[str, Any], params: Sequence[tuple[str, str]]) -> bool | None:
    """Whether a minted refund belongs on a filtered page: True, False, or None for "cannot tell".

    None is the case the fixture forces (#43). A refund posted with only `charge` is answered from
    a fixture whose `payment_intent` is `null`, so a read filtered by `payment_intent=pi_X` cannot
    be told from the minted object whether the refund matches - in production Stripe would have
    filled that field in. Treating unknown as "does not match" dropped the agent's own refund off
    a page that then claimed to be complete, which is the untruth this module's header forbids. The
    caller turns None into `partial` instead.
    """
    for name, value in params:
        if name not in ("charge", "payment_intent"):
            continue
        mine = refund.get(name)
        if mine is None:
            return None
        if mine != value:
            return False
    return True


def _limit(params: Sequence[tuple[str, str]]) -> int:
    for name, value in reversed(list(params)):
        if name != "limit":
            continue
        try:
            asked = int(value)
        except ValueError:
            return DEFAULT_LIST_LIMIT
        return asked if 1 <= asked <= MAX_LIST_LIMIT else DEFAULT_LIST_LIMIT
    return DEFAULT_LIST_LIMIT


# ------------------------------------------------------------------------ the L3 preconditions
#
# Whether Stripe would have accepted a refund at all, decided against the charge it names (#45).
# The charge is the OVERLAID one: the policy fetches it live and applies this run's own faked
# refunds to it with `_charge` before `_refund_verdict` sees it. That is the point of checking it
# here rather than trusting the live body alone - a second refund of a charge the run already
# refunded in full is one Stripe would refuse, and the live charge cannot know about the first,
# because the first was never made.

# Stripe sends no `code` for the amount-exceeds error - the message and `param: amount` are the
# whole of it - so this is irimi's own label for the summary line, and it is deliberately NOT
# written into the modeled body (#45).
AMOUNT_TOO_LARGE = "amount_too_large"
CHARGE_ALREADY_REFUNDED = "charge_already_refunded"


def _refund_probe(proposal: Proposal) -> Probe | None:
    """`GET /v1/charges/{charge}` for the charge this refund names.

    A refund posted with `payment_intent` and no `charge` is not checked: resolving the intent to
    its charge is a second read and a second modeled object, neither of which is in Phase 2's
    table, and guessing would be worse than saying so (#45).
    """
    charge = proposal.posted.get("charge")
    # Not URL-quoted: a Stripe id is `[A-Za-z0-9_]` only, and a value that is not is one the real
    # service would refuse. One that would change the path or start a query is not probed at all.
    if not isinstance(charge, str) or not charge or "/" in charge or "?" in charge:
        return None
    return Probe(operation="charges.retrieve", method="GET", path=f"/v1/charges/{charge}")


def _refund_verdict(proposal: Proposal, document: Any) -> Rejection | NotEvaluable | None:
    """`charge_already_refunded`, the amount-exceeds error, `NOT_EVALUABLE`, or None."""
    if (
        not isinstance(document, dict)
        or document.get("object") != "charge"
        or "amount_refunded" not in document
    ):
        # irimi never rejects on a body it did not understand: a false rejection is the untruth
        # this whole path exists to prevent. Nor does it pass on one - that would claim the write
        # was checked against a charge irimi never read. Saying so is `NOT_EVALUABLE` (#45).
        return NOT_EVALUABLE
    charge_id = proposal.posted.get("charge")
    if document.get("refunded") is True:
        return Rejection(
            status=400,
            code=CHARGE_ALREADY_REFUNDED,
            body={
                "error": {
                    "code": CHARGE_ALREADY_REFUNDED,
                    "doc_url": "https://stripe.com/docs/error-codes/charge-already-refunded",
                    "message": f"Charge {charge_id} has already been refunded.",
                    "type": "invalid_request_error",
                }
            },
        )
    amount = proposal.posted.get("amount")
    charged, refunded = document.get("amount"), document.get("amount_refunded")
    # Both sides of the subtraction must be the charge's own numbers. `_int` reads a missing or
    # malformed one as 0, which would make every refund "too large" - a false rejection (#45).
    if not (_is_int(charged) and _is_int(refunded)):
        return NOT_EVALUABLE
    if not _is_int(amount):
        # A refund posting no `amount` is Stripe's "refund whatever is left", which is never over
        # the remaining amount; one posting a malformed amount is the real service's to refuse,
        # not irimi's to guess at. Neither is a failure to evaluate the charge (#45).
        return None
    remaining = _int(charged) - _int(refunded)
    if _int(amount) > remaining:
        # The prose is irimi's: minor-unit formatting lives in `report`, and this package stays
        # pure and below it. The SHAPE is Stripe's - `invalid_request_error` with `param: amount` -
        # which is what makes stripe-python raise `InvalidRequestError`.
        currency = str(document.get("currency", "")).upper()
        message = (
            f"Refund amount ({amount} {currency}) is greater than unrefunded amount on charge "
            f"({remaining} {currency})"
        )
        return Rejection(
            status=400,
            code=AMOUNT_TOO_LARGE,
            body={
                "error": {"message": message, "param": "amount", "type": "invalid_request_error"}
            },
        )
    return None


CHARGE_REFUNDABLE = Check(probe=_refund_probe, verdict=_refund_verdict)
