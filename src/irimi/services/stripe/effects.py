"""What a faked Stripe write does to later live reads.

    refunds.create          a refund object (`refunds.retrieve` of its id answers 200, not 404);
                            the charge's `amount_refunded`, `refunded` and expanded `refunds`, in
                            `charges.retrieve` and `charges.list`; page 1 of the refunds list
    customers.update        the posted fields, on the customer
    payment_intents.cancel  `status`, `canceled_at`, `cancellation_reason`

Guarantees:
- No field the live object lacks is added.
- What cannot be applied whole leaves the body as Stripe sent it and marks the read `partial`.
- Pure functions over plain data (no clock, I/O or module state), so a replay reuses them.
"""

from collections.abc import Sequence
from typing import Any
from urllib.parse import parse_qsl, urlencode

from irimi.core.bodies import is_int
from irimi.core.exchange import Request
from irimi.services.model import SERVICE_OWNED, Applied, Read, Rewritten, Write

# Stripe's own default page size, and what a list read gets when it names no `limit`.
DEFAULT_LIST_LIMIT = 10
MAX_LIST_LIMIT = 100
# `GET /v1/refunds` parameters understood (else `partial`); `expand[0]` is matched by prefix.
KNOWN_LIST_PARAMS = frozenset({"limit", "charge", "payment_intent"})
CURSOR_PARAMS = frozenset({"starting_after", "ending_before"})


def apply_read(read: Read, document: Any, writes: Sequence[Write]) -> Applied:
    """Apply the run's faked Stripe writes to one live read's parsed body."""
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
    """Drop a `starting_after` naming a minted refund, which Stripe would refuse, before forwarding.

    A minted refund is the newest, so the page after it is the real list from its top.
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


def _charge(charge: dict[str, Any], writes: Sequence[Write]) -> Applied:
    """Move `amount_refunded`, `refunded` and the expanded `refunds` list on one charge."""
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
    # Stripe sends `refunds` only when the request expanded it.
    if isinstance(refunds, dict) and isinstance(refunds.get("data"), list):
        refunds["data"] = [r.answer for r in reversed(mine)] + refunds["data"]
        if isinstance(refunds.get("total_count"), int):
            refunds["total_count"] += len(mine)
    return Applied(charge, changed=True)


def _charges_list(document: dict[str, Any], writes: Sequence[Write]) -> Applied:
    """Apply the charge effect to every charge in the list, so it agrees with a retrieve."""
    data = document.get("data")
    if not isinstance(data, list):
        return Applied(document)
    changed = False
    for item in data:
        if isinstance(item, dict) and _charge(item, writes).changed:
            changed = True
    return Applied(document, changed=changed)


def _refunds_list(read: Read, document: dict[str, Any], writes: Sequence[Write]) -> Applied:
    """Put minted refunds first on page 1 of `GET /v1/refunds`, keeping the page to its limit."""
    minted = _minted_refunds(writes)
    if not minted:
        return Applied(document)
    params = parse_qsl(read.request.query, keep_blank_values=True)
    if any(k in CURSOR_PARAMS for k, _ in params):
        # A later page's real items depend on where the minted refund landed.
        return Applied(document, partial=True)
    removed = read.translated_from or ""
    if removed.startswith("starting_after="):
        # This page follows a minted refund, which is on the page before. Only an older minted
        # refund, which the real list leaves out, would belong here: then the page is `partial`.
        ids = [r.answer["id"] for r in minted]
        after = removed.partition("=")[2]
        older = minted[: ids.index(after)] if after in ids else []
        if any(_passes_filters(r.answer, params) is not False for r in older):
            return Applied(document, partial=True)
        return Applied(document)
    if any(not _known_list_param(k) for k, _ in params):
        return Applied(document, partial=True)
    verdicts = [(r, _passes_filters(r.answer, params)) for r in minted]
    if any(passes is None for _, passes in verdicts):
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
    """Answer a minted refund from the write log, as a copy at 200; other errors stay, `partial`."""
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
    """Apply the posted fields of every `customers.update` for this customer."""
    changed = False
    for write in writes:
        if write.operation != "customers.update" or write.answer.get("id") != customer.get("id"):
            continue
        for name, posted in write.posted.items():
            # A field the live object lacks is never added; this also skips `expand[0]`.
            if name in SERVICE_OWNED or name not in customer:
                continue
            value = posted
            if name == "metadata" and isinstance(posted, dict) and isinstance(customer[name], dict):
                value = {**customer[name], **posted}  # Stripe merges metadata
            # An update that moves no field must not mark the live body `overlay`.
            if customer[name] != value:
                customer[name] = value
                changed = True
    return Applied(customer, changed=changed)


def _payment_intent(intent: dict[str, Any], writes: Sequence[Write]) -> Applied:
    """Set `status`, `canceled_at` and `cancellation_reason` on a cancelled payment intent."""
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


def _minted_refunds(writes: Sequence[Write]) -> list[Write]:
    return [
        w for w in writes if w.operation == "refunds.create" and isinstance(w.answer.get("id"), str)
    ]


def minor_units(value: Any) -> int:
    """Return `value` as a whole number of minor units, or 0 (a `bool` counts as 0)."""
    return value if is_int(value) else 0


def _known_list_param(name: str) -> bool:
    return name in KNOWN_LIST_PARAMS or name == "expand" or name.startswith("expand[")


def _passes_filters(refund: dict[str, Any], params: Sequence[tuple[str, str]]) -> bool | None:
    """Return whether a minted refund passes the page's filters; None where its field is `null`."""
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
