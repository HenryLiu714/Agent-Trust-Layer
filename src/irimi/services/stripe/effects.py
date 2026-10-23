"""Stripe's L2 effects: what a faked write does to a later live read.

The modelled effects, by the write that causes each one:

    refunds.create          a new refund object; on the charge, `amount_refunded` and `refunded`,
                            and the expanded `refunds` list; on page 1 of the refunds list
    customers.update        the posted fields, on the customer
    payment_intents.cancel  `status`, `canceled_at`, `cancellation_reason`

Two reads beyond the table's letter, both so that irimi does not contradict itself:
`charges.list` gets the same charge effect as `charges.retrieve`, because the refund agent finds its
charge in the list and a list that ignored the refund would disagree with the retrieve; and
`refunds.retrieve` of a minted id is answered from the write log with the object irimi minted, over
whatever Stripe said about it - the one read where the seam replaces a STATUS as well as a body,
because Stripe has never heard of that id and answers `404`.

Two rules run through all of it:

* **Never invent a field the live object does not have.** A field Stripe's own response omits is
  one the agent can never see in production, and putting one there hands it a body no live read
  can produce. This is `generic.reflect_over`'s rule, applied to reads.
* **Say `partial` rather than half-apply.** A page the table does not model, a filter it cannot
  read, a cursor page whose real contents depend on where the minted refund landed: the document is
  left exactly as Stripe sent it and the exchange records that the world irimi showed is
  incomplete. Answering a read outright is the opposite move and needs the opposite justification -
  see `_refund_retrieve`, where there is no live object to be incomplete about.

Pure functions over plain data: no clock, no minting, no I/O, no module state. A replay
runs these same functions over a recording.
"""

from collections.abc import Sequence
from typing import Any
from urllib.parse import parse_qsl, urlencode

from irimi.bodies import is_int
from irimi.exchange import Request
from irimi.services.model import SERVICE_OWNED, Applied, Read, Rewritten, Write

# Stripe's own default page size, and what a list read gets when it names no `limit`.
DEFAULT_LIST_LIMIT = 10
MAX_LIST_LIMIT = 100
# The `GET /v1/refunds` parameters the effects understand. Anything else means irimi cannot say
# where the minted refund belongs on this page, and the read is `partial`. `expand` arrives as
# `expand[0]`, so it is matched by prefix.
KNOWN_LIST_PARAMS = frozenset({"limit", "charge", "payment_intent"})
CURSOR_PARAMS = frozenset({"starting_after", "ending_before"})


def apply_read(read: Read, document: Any, writes: Sequence[Write]) -> Applied:
    """The run's faked Stripe writes, applied to one live read's parsed body."""
    # `read.posted` is ignored, not forgotten: a Stripe read is a GET and posts nothing.
    operation = read.operation
    if not isinstance(document, dict):
        return Applied(document)
    if operation == "charges.retrieve":
        return _charge(document, writes)
    if operation == "charges.list":
        return _charges_list(document, writes)
    if operation == "refunds.list":
        return _refunds_list(read, document, writes)
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
    refunded = minor_units(charge.get("amount_refunded")) + sum(
        minor_units(r.answer.get("amount")) for r in mine
    )
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


def _refunds_list(read: Read, document: dict[str, Any], writes: Sequence[Write]) -> Applied:
    """Page 1 of `GET /v1/refunds`: the minted refund first, the page kept to its own limit."""
    minted = _minted_refunds(writes)
    if not minted:
        return Applied(document)
    params = parse_qsl(read.request.query, keep_blank_values=True)
    if any(k in CURSOR_PARAMS for k, _ in params):
        # Some page other than the first, and which real items it holds depends on where the
        # minted refund landed. The effects model page 1 only.
        return Applied(document, partial=True)
    removed = read.translated_from or ""
    if removed.startswith("starting_after="):
        # `rewrite_query` removed a cursor naming a refund irimi minted, so this IS the page that
        # follows it: the minted refund belongs on the page before this one, and prepending it
        # here would hand the agent its own refund twice. Fully modeled, nothing to change -
        # unless an OLDER minted refund also belongs after the cursor, which the real list from
        # its top leaves out. That page is not modeled, and saying `full` would hide it.
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
    """A refund irimi minted, answered from the write log rather than left as Stripe's 404.

    Stripe has never heard of that id and answers `404` with `resource_missing`, so this is the one
    read where the honest answer needs a STATUS as well as a body - `Applied.status` is how the
    effects say so. The object handed back is the very one
    irimi answered the write with, which is what the agent was told exists: nothing is invented,
    and none of the live body survives because there is no live object behind it.

    Whatever Stripe said ABOUT THE OBJECT, not only a 404: a 24-character random id colliding with a
    real refund is not a scenario, and the object the agent asked for is the one irimi minted. But
    not over an error about anything else. A 401, a 429 or a 5xx on the same read is Stripe's answer
    about the credentials or itself, which production would have sent for the real refund too, and
    answering 200 over it is the error-into-success untruth this module's header forbids. So an
    `error` body whose code is not `resource_missing` is left as sent and flagged `partial`.

    A shallow copy, never the write log's own dict - the overlay serializes what it is handed and a
    later effect must not find this one mutated. `slack._posts` copies for the same reason.

    A read of a refund irimi did NOT mint is untouched, 404 and all: that one is Stripe's own
    answer about Stripe's own state, and the agent should see it.
    """
    ident = request.path.rsplit("/", 1)[-1]
    mine = next((r for r in _minted_refunds(writes) if r.answer["id"] == ident), None)
    if mine is None:
        return Applied(document)
    error = document.get("error")
    if error is not None and not (
        isinstance(error, dict) and error.get("code") == "resource_missing"
    ):
        return Applied(document, partial=True)
    return Applied(dict(mine.answer), changed=True, status=200)


def _customer(customer: dict[str, Any], writes: Sequence[Write]) -> Applied:
    """The posted fields of every `customers.update` for this customer."""
    changed = False
    for write in writes:
        if write.operation != "customers.update" or write.answer.get("id") != customer.get("id"):
            continue
        for name, value in write.posted.items():
            # `name not in customer` is `generic.reflect_over`'s rule: a field the live object
            # does not have is one the real service would not have returned. It also keeps
            # `expand[0]` and friends out, without a second list of parameter names to maintain.
            if name in SERVICE_OWNED or name not in customer:
                continue
            if name == "metadata" and isinstance(value, dict) and isinstance(customer[name], dict):
                value = {**customer[name], **value}  # Stripe merges metadata
            # `changed` only when a field really moved: an update whose every field was skipped
            # leaves the live body as Stripe sent it, and must not be stamped `overlay`.
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


def minor_units(value: Any) -> int:
    """`value` as a whole number of minor units, or 0. `bool` is an `int` and is not one here."""
    return value if is_int(value) else 0


def _known_list_param(name: str) -> bool:
    return name in KNOWN_LIST_PARAMS or name == "expand" or name.startswith("expand[")


def _passes_filters(refund: dict[str, Any], params: Sequence[tuple[str, str]]) -> bool | None:
    """Whether a minted refund belongs on a filtered page: True, False, or None for "cannot tell".

    None is the case the fixture forces. A refund posted with only `charge` is answered from
    a fixture whose `payment_intent` is `null`, so a read filtered by `payment_intent=pi_X` cannot
    be told from the minted object whether the refund matches - in production Stripe would have
    filled that field in. Treating unknown as "does not match" would drop the agent's own refund
    off a page that then claims to be complete, which is the untruth this module's header forbids.
    The caller turns None into `partial` instead.
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
