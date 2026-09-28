"""Redaction before anything reaches disk (#69, design §5.3, D9).

From the trace store (#70) on, irimi writes what the agent sent and what it was sent back:
`Authorization` headers, Slack tokens, webhook URLs whose path IS the secret. This module is the
copy that goes to disk and nothing else. The live flow and the agent's answer are never changed;
the store's writer thread calls `redact_exchange` and `redact_json`, the engine never does.

Redaction is always on and keyed: a secret becomes `<redacted:` + 16 hex digits of an HMAC-SHA256
under this install's key + `>`, so the same secret always becomes the same placeholder and a
Phase 5 replay can still match a request against its recording. The key is per install
(`$IRIMI_HOME/redact.key`), so two machines give one secret two placeholders; whether recordings
move between machines is a Phase 6 decision (push/pull), and `docs/trace-format.md` says so.

It is also where "this header is a credential" is stated, once: an answer target may not be handed
one (`delegation`, #32) and disk may not be handed one either.

FAILURE IS CLOSED. A value that cannot be redacted is stored as `<redaction-failed>`, never as it
was, and the exchange gains `REDACTION_FAILED_FLAG`. `redact_exchange` and `redact_json` never
raise. Layer 2: it imports `exchange`, `paths`, `bodies`, `servicemap` and `trace`.
"""

import hmac
import json
import logging
import os
import re
import secrets
from collections.abc import Callable
from dataclasses import replace
from functools import partial
from pathlib import Path
from urllib.parse import quote_plus, unquote_plus, urlsplit

from irimi import paths
from irimi.bodies import FORM_CT
from irimi.exchange import (
    REDACTION_FAILED_FLAG,
    Exchange,
    Headers,
    Request,
    Response,
    header_value,
    media_type,
)
from irimi.servicemap import CREDENTIAL_PATH_HOSTS
from irimi.trace import JSONValue

logger = logging.getLogger(__name__)

# Header names that carry a credential. An answer target is not handed them unless the route sets
# `forward_auth: true` (#32), and disk is never handed them (#69). Stripping `Authorization` alone
# was narrower than the rule it implements - "a local stub does not need your real key" - and left
# `Cookie`, `x-api-key` and `DD-API-KEY` on the request (#32).
#
# A marker list rather than a vendor list, because the vendor list is never finished: every new
# service brings its own spelling, and the one it is missing is the one that leaks. Over-matching
# costs a stub a header it probably did not want, and a stored run a header's real value;
# under-matching hands a live key to a stub or to disk, so the rule is deliberately wide.
CREDENTIAL_MARKERS: tuple[str, ...] = (
    "auth",  # authorization, proxy-authorization, x-sentry-auth, x-authenticated-*
    "api-key",  # x-api-key, dd-api-key, x-goog-api-key
    "api_key",
    "apikey",
    "token",  # x-auth-token, x-amz-security-token, x-csrf-token
    "secret",
    "credential",
    "password",
    "signature",  # x-slack-signature and friends: a signature over a shared secret
)
# The ones no marker catches: a cookie jar is a credential, and these two vendor headers are
# spelled with none of the words above.
CREDENTIAL_HEADERS: frozenset[str] = frozenset({"cookie", "dd-application-key", "x-honeycomb-team"})

# The key is 32 random bytes, the HMAC-SHA256 block's worth of secret.
KEY_BYTES = 32
KEY_MODE = 0o600

# What a value that could not be redacted is stored as (#69, rule 7). Never the original.
REDACTION_FAILED = "<redaction-failed>"

# Names whose value is a secret wherever they appear: a query parameter, a form field (the last
# bracket segment, so `card[token]`), a JSON object key at any depth. Compared case-insensitively
# and whole, so `max_tokens`, `total_tokens` and `token_count` are not secrets.
SECRET_KEYS: frozenset[str] = frozenset(
    {
        "api_key",
        "apikey",
        "secret",
        "client_secret",
        "password",
        "access_token",
        "refresh_token",
        "id_token",
        "token",
    }
)

# Credential shapes, replaced wherever they appear in text irimi can read: header values, the
# path, query and form values, JSON strings, and a UTF-8 body that is neither JSON nor a form.
SECRET_PATTERNS: tuple[str, ...] = (
    r"sk_(live|test)_[A-Za-z0-9]+",
    r"rk_(live|test)_[A-Za-z0-9]+",
    r"whsec_[A-Za-z0-9]+",
    r"xox[abprs]-[A-Za-z0-9-]+",
    r"xapp-[A-Za-z0-9-]+",
    r"gh[pousr]_[A-Za-z0-9]{36,}",
    r"github_pat_[A-Za-z0-9_]+",
    r"AKIA[0-9A-Z]{16}",
    r"sk-ant-[A-Za-z0-9_-]+",
    r"sk-(proj-)?[A-Za-z0-9_-]{20,}",
)
# One pass over the text, the patterns tried in the order above at each position, so `sk-ant-` is
# replaced whole before the wider `sk-` could claim it. A shape must not follow a letter or a
# digit: without that, `task_test_runner` stored as `ta<redacted:…>` and any slug holding `risk-`
# followed by 20 more characters lost its tail. A real key follows a space, a quote, `=`, `/`, `_`
# or the start of the value, and still matches.
_SECRET_SHAPE = re.compile(r"(?<![A-Za-z0-9])(?:" + "|".join(SECRET_PATTERNS) + ")")

# What `quote_plus` leaves alone in a redacted query or form value, so the placeholder stays
# readable on disk, as it is everywhere else.
_PLACEHOLDER_SAFE = "<>:"


class RedactKeyError(ValueError):
    """`redact.key` exists and is not a key irimi wrote. str(exc) says what to do about it."""


def is_credential_header(name: str) -> bool:
    """True when this header's value is a credential: a target must not be handed it (#32), and
    disk must not either (#69).

    Compared case-insensitively and by substring, so a vendor header nobody has written down yet
    (`x-acme-api-key`) is covered the day it appears. See CREDENTIAL_MARKERS for why the rule is
    wide rather than exact.
    """
    lowered = name.strip().lower()
    return lowered in CREDENTIAL_HEADERS or any(m in lowered for m in CREDENTIAL_MARKERS)


def placeholder(key: bytes, value: str) -> str:
    """The stored stand-in for `value`: the same value under the same key always gives the same
    one, which is what lets a replay match a request against its recording (design §5.3)."""
    return "<redacted:" + hmac.new(key, value.encode(), "sha256").hexdigest()[:16] + ">"


def load_key(home: Path) -> bytes:
    """This install's redaction key, `home/redact.key`, created on first use.

    Raises RedactKeyError when the file is there and is not 32 bytes: a key irimi did not write
    would give every secret a placeholder no earlier recording shares, and silently replacing it
    would do the same to every recording already on disk.
    """
    path = home / paths.REDACT_KEY_NAME
    if not path.exists():
        _create_key(path)
    key = path.read_bytes()
    if len(key) != KEY_BYTES:
        raise RedactKeyError(
            f"{path} holds {len(key)} bytes, not {KEY_BYTES}; delete it to create a new key "
            "(runs recorded under the old key will no longer match new ones)"
        )
    return key


def _create_key(path: Path) -> None:
    """Publish a new key at `path`, unless another process publishes one first.

    The key is written to a file of its own, created with O_CREAT | O_EXCL and mode 0600, and then
    hard-linked to `path`. `os.link` never replaces an existing file, so the first process to link
    wins, and `path` never exists half-written: a process that loses the race reads the winner's
    whole key, where creating `path` directly could hand it an empty file.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{secrets.token_hex(8)}.tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, KEY_MODE)
    try:
        with os.fdopen(fd, "wb") as f:
            os.fchmod(fd, KEY_MODE)  # O_CREAT's mode is subject to the umask
            f.write(secrets.token_bytes(KEY_BYTES))
            f.flush()
            os.fsync(fd)
        try:
            os.link(tmp, path)
        except FileExistsError:
            pass  # another process published first, and its key is this install's key
    finally:
        tmp.unlink(missing_ok=True)


def redact_exchange(exchange: Exchange, key: bytes) -> Exchange:
    """A new Exchange, safe to write to disk. Never raises; `exchange` is not changed.

    The request's path, query, headers and body, the response's headers and body, and the answer
    target are redacted. Every other field is copied with `dataclasses.replace`, so a field added
    to Exchange later survives without a change here. A part that could not be redacted is stored
    as REDACTION_FAILED and the copy gains REDACTION_FAILED_FLAG.
    """
    guard = _Guard()
    request = _redact_request(exchange.request, key, guard)
    response = exchange.response
    if response is not None:
        response = _redact_response(response, key, guard)
    target = guard(
        "the answer target",
        REDACTION_FAILED,
        partial(_redact_target, exchange.target, exchange.request.host, key),
    )
    flags = exchange.flags
    if guard.failed and REDACTION_FAILED_FLAG not in flags:
        flags = (*flags, REDACTION_FAILED_FLAG)
    return replace(exchange, request=request, response=response, target=target, flags=flags)


def redact_json(value: JSONValue, key: bytes) -> JSONValue:
    """`value` with every secret key's value and every secret shape replaced: a trigger's args and a
    tool call's args and result (#70). Never raises; REDACTION_FAILED when it could not redact."""
    failed: JSONValue = REDACTION_FAILED
    return _Guard()("a JSON value", failed, partial(_redact_json, value, key))


class _Guard:
    """Runs one redaction at a time, and remembers whether any of them had to fail closed."""

    def __init__(self) -> None:
        self.failed = False

    def __call__[T](self, what: str, fallback: T, step: Callable[[], T]) -> T:
        try:
            return step()
        except Exception as exc:
            self.failed = True
            # The exception's type and never its message: a UnicodeEncodeError quotes the value.
            logger.warning(
                "irimi: could not redact %s (%s); stored %s instead",
                what,
                type(exc).__name__,
                REDACTION_FAILED,
            )
            return fallback


def _redact_request(request: Request, key: bytes, guard: _Guard) -> Request:
    return replace(
        request,
        path=guard(
            "the request path",
            "/" + REDACTION_FAILED,
            partial(_redact_path, request.host, request.path, key),
        ),
        query=guard("the query", REDACTION_FAILED, partial(_redact_urlencoded, request.query, key)),
        headers=_redact_headers(request.headers, key, guard, is_response=False),
        body=guard(
            "the request body",
            REDACTION_FAILED.encode(),
            partial(_redact_body, request.body, _content_type(request.headers), key),
        ),
    )


def _redact_response(response: Response, key: bytes, guard: _Guard) -> Response:
    return replace(
        response,
        headers=_redact_headers(response.headers, key, guard, is_response=True),
        body=guard(
            "the response body",
            REDACTION_FAILED.encode(),
            partial(_redact_body, response.body, _content_type(response.headers), key),
        ),
    )


def _content_type(headers: Headers) -> str:
    return media_type(header_value(headers, "content-type") or "")


def _redact_headers(headers: Headers, key: bytes, guard: _Guard, is_response: bool) -> Headers:
    what = "a response header" if is_response else "a request header"
    return tuple(
        (name, guard(what, REDACTION_FAILED, partial(_redact_header, name, v, key, is_response)))
        for name, v in headers
    )


def _redact_header(name: str, value: str, key: bytes, is_response: bool) -> str:
    """A credential header's whole value is hidden; `Set-Cookie` is one on a response only."""
    if is_credential_header(name) or (is_response and name.strip().lower() == "set-cookie"):
        return _hide(value, key)
    return _redact_text(value, key)


def _redact_path(host: str, path: str, key: bytes) -> str:
    """On a credential-path host (`hooks.slack.com`) the path IS the secret, so all of it goes."""
    if host in CREDENTIAL_PATH_HOSTS:
        return "/" + placeholder(key, path)
    return _redact_text(path, key)


def _redact_target(target: str, host: str, key: bytes) -> str:
    """An answer target's URL under the request's rules. A bare-origin target keeps the request's
    path (`delegation.target_url`), so a webhook's secret path would otherwise reach disk here."""
    if not target:
        return target
    parts = urlsplit(target)
    path = _redact_path(host, parts.path, key) if parts.path else parts.path
    redacted = parts._replace(path=path, query=_redact_urlencoded(parts.query, key)).geturl()
    return _redact_text(redacted, key)


def _redact_urlencoded(text: str, key: bytes) -> str:
    """A query string or a form body, pair by pair, then scanned whole for secret shapes. A pair
    nothing matched keeps its bytes, and `text` itself comes back when nothing matched at all.

    The whole-text scan is what catches a shape in a field's NAME, and in a body that only claims
    to be a form: `{"key": "sk_live_…"}` sent as one splits into a single pair whose name is the
    whole document and whose value is empty.
    """
    pairs = text.split("&")
    redacted = [_redact_pair(pair, key) for pair in pairs]
    if not all(new is old for new, old in zip(redacted, pairs, strict=True)):
        text = "&".join(redacted)
    return _redact_text(text, key)


def _redact_pair(pair: str, key: bytes) -> str:
    raw_name, _, raw_value = pair.partition("=")
    value = unquote_plus(raw_value)
    if _is_secret_key(_last_segment(unquote_plus(raw_name))):
        new = _hide(value, key)
    else:
        new = _redact_text(value, key)
    if new is value:
        return pair
    return f"{raw_name}={quote_plus(new, safe=_PLACEHOLDER_SAFE)}"


def _last_segment(name: str) -> str:
    """`card[token]` -> `token`; `a[b][c]` -> `c`; `token` -> `token`."""
    return name.rstrip("]").rpartition("[")[2]


def _redact_body(body: bytes, content_type: str, key: bytes) -> bytes:
    """`body` itself unless a rule changed something, so a body with nothing to hide is stored
    byte for byte: its key order, its whitespace, its escapes.

    A body that is not UTF-8 is stored unscanned - a known limitation, in
    `docs/trace-format.md`. A form body is read as a form because its content type says so; any
    other body that parses as JSON is walked as JSON, whatever its content type claims; the rest
    is scanned as text, which is what an SSE stream is.

    Only a body that is not JSON at all is scanned as text. JSON that `json.loads` refuses for any
    other reason (an integer past its digit limit) raises to the guard and fails closed, because
    a text scan would miss a secret key's value. So does an object with a repeated key: `loads`
    keeps the last value, and `{"token": "…", "token": null}` would come back byte for byte.
    """
    if not body:
        return body
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError:
        return body
    if content_type == FORM_CT:
        redacted = _redact_urlencoded(text, key)
        return body if redacted is text else redacted.encode()
    try:
        document = json.loads(text, object_pairs_hook=_unique_members)
    except json.JSONDecodeError:
        redacted = _redact_text(text, key)
        return body if redacted is text else redacted.encode()
    walked = _redact_json(document, key)
    if walked is document:
        return body
    return json.dumps(walked, separators=(",", ":"), ensure_ascii=False).encode()


def _unique_members(pairs: list[tuple[str, JSONValue]]) -> dict[str, JSONValue]:
    """A JSON object's members as a dict, refusing a repeated key, whose earlier values the walk
    would never see (#69)."""
    members = dict(pairs)
    if len(members) != len(pairs):
        raise ValueError("a JSON object repeats a key")
    return members


def _redact_json(value: JSONValue, key: bytes) -> JSONValue:
    """`value` itself when nothing in it matched, else a copy; the identity is the signal."""
    if isinstance(value, str):
        return _redact_text(value, key)
    if isinstance(value, list):
        items = [_redact_json(item, key) for item in value]
        if all(new is old for new, old in zip(items, value, strict=True)):
            return value
        return items
    if isinstance(value, dict):
        pairs = [(_redact_text(k, key), _redact_member(k, v, key)) for k, v in value.items()]
        if all(nk is k and nv is v for (nk, nv), (k, v) in zip(pairs, value.items(), strict=True)):
            return value
        return dict(pairs)
    return value


def _redact_member(name: str, value: JSONValue, key: bytes) -> JSONValue:
    """A secret key's value is hidden whole: a string as itself, anything else as its JSON with
    sorted keys, so two spellings of one object hide the same. `null` hides nothing and stays."""
    if not _is_secret_key(name):
        return _redact_json(value, key)
    if value is None:
        return value
    return _hide(value if isinstance(value, str) else json.dumps(value, sort_keys=True), key)


def _is_secret_key(name: str) -> bool:
    return name.lower() in SECRET_KEYS


def _hide(value: str, key: bytes) -> str:
    """A secret's placeholder. An empty value hides nothing and stays empty: a placeholder would
    turn "no token was sent" into "a token was sent"."""
    return value if value == "" else placeholder(key, value)


def _redact_text(text: str, key: bytes) -> str:
    """`text` itself when no secret shape is in it, else a copy with each one replaced."""
    redacted, count = _SECRET_SHAPE.subn(lambda m: placeholder(key, m.group(0)), text)
    return redacted if count else text
