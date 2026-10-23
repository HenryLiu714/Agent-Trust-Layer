"""Redaction of the copy of each event and run record that goes to disk.

Guarantees:
- Nothing unredacted on disk: the store writes only what `for_disk` and `run_for_disk` return.
  The live flow is never changed; only the store's writer thread calls them.
- Keyed and stable: a secret becomes `<redacted:` + 16 hex digits of HMAC-SHA256 + `>` under the
  install's key (`$IRIMI_HOME/redact.key`), so a replay can match a request to its recording.
  The key is per install, so a recording does not match on another machine.
- Fails closed: a value that cannot be redacted is stored as `<redaction-failed>` and the exchange
  is flagged `redaction-failed`. No public function raises.
- `is_credential_header` also says which headers a target may not receive.
"""

import hmac
import json
import logging
import os
import re
import secrets
import stat
from collections.abc import Callable
from dataclasses import replace
from functools import partial
from itertools import pairwise
from pathlib import Path
from urllib.parse import quote_plus, unquote_plus, urlsplit

from irimi.config import paths
from irimi.config.files import write_private
from irimi.core.bodies import FORM_CT
from irimi.core.exchange import Exchange, Flag, Headers, Request, Response, header_value, media_type
from irimi.core.secret_shapes import SECRET_SHAPE
from irimi.protocol.trace import (
    ErrorInfo,
    Event,
    JSONValue,
    RunRecord,
    TelemetrySeen,
    ToolCall,
    Trigger,
)
from irimi.servicemaps import shipped_credential_hosts

logger = logging.getLogger(__name__)

# Substrings that mark a header name as a credential. Markers rather than a vendor list, which is
# never finished; the rule is deliberately wide, since under-matching leaks a live key.
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
# Credential headers no marker catches.
CREDENTIAL_HEADERS: frozenset[str] = frozenset({"cookie", "dd-application-key", "x-honeycomb-team"})

KEY_BYTES = 32

REDACTION_FAILED = "<redaction-failed>"

# Names whose value is a secret as a query parameter, a form field's last bracket segment
# (`card[token]`) or a JSON key at any depth. Matched whole, so `max_tokens` is not one.
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

# Credential shapes (`secret_shapes.SECRET_PATTERNS`) are replaced in all readable text.

# A header value that is a URL is redacted as one: `Location` may carry an OAuth token in its
# query or fragment, in no recognisable shape.
_ABSOLUTE_URL = re.compile(r"\s*https?://", re.IGNORECASE)

_SSE_DATA = "data:"

# A line with its ending: CRLF, LF or a lone CR, as SSE allows. A CR-only stream split at LF
# would be one non-JSON line, and its JSON tokens would be missed.
_LINE = re.compile(r"[^\r\n]*(?:\r\n|\r|\n)|[^\r\n]+")

# `_LINE` over bytes, for `_rechunk`, since chunk lengths count bytes. In UTF-8, CR and LF are
# never part of a longer character, so both split alike.
_BYTE_LINE = re.compile(_LINE.pattern.encode())


def complete_lines(body: bytes) -> bytes:
    """Return `body` through its last complete line, for redaction to read. Never raises."""
    return body[: max(body.rfind(b"\n"), body.rfind(b"\r")) + 1]


# Left unquoted in a redacted query or form value, so the placeholder stays readable.
_PLACEHOLDER_SAFE = "<>:"


class RedactKeyError(ValueError):
    """`redact.key` exists and is not a key irimi wrote. str(exc) says what to do about it."""


def is_credential_header(name: str) -> bool:
    """Return True when this header carries a credential (case-insensitive substring match)."""
    lowered = name.strip().lower()
    return lowered in CREDENTIAL_HEADERS or any(m in lowered for m in CREDENTIAL_MARKERS)


def placeholder(key: bytes, value: str) -> str:
    """Return the stored placeholder for `value`, the same for the same value and key."""
    return "<redacted:" + hmac.new(key, value.encode(), "sha256").hexdigest()[:16] + ">"


def load_key(home: Path) -> bytes:
    """Return the install's redaction key, `home/redact.key`, creating it on first use.

    Raises RedactKeyError when the path is not a regular file or not 32 bytes; replacing it
    silently would orphan every recording. A symlink to a regular file is followed, so a shared
    key can be mounted (a Kubernetes Secret volume is a tree of symlinks).
    """
    path = home / paths.REDACT_KEY_NAME
    if not os.path.lexists(path):
        _create_key(path)
    try:
        regular = stat.S_ISREG(path.stat().st_mode)
    except FileNotFoundError:
        regular = False
    if not regular:
        raise RedactKeyError(
            f"{path} is not a regular file (a directory or a dangling symlink); replace it with "
            "the key file, or remove it to create a new key"
        )
    key = path.read_bytes()
    if len(key) != KEY_BYTES:
        raise RedactKeyError(
            f"{path} holds {len(key)} bytes, not {KEY_BYTES}; delete it to create a new key "
            "(runs recorded under the old key will no longer match new ones)"
        )
    return key


def _create_key(path: Path) -> None:
    """Publish a new key at `path`, unless another process publishes one first.

    Written whole, then hard-linked into place: the first link wins and `path` is never
    half-written. Without hard links the OSError is raised; `os.rename` would overwrite a winner.
    """
    write_private(path, secrets.token_bytes(KEY_BYTES), how="publish", durable=True)


def redact_exchange(exchange: Exchange, key: bytes) -> Exchange:
    """Return a copy of `exchange` safe to write to disk. Never raises.

    Redacts the request's path, query, headers and body, the response's headers and body, the
    target and the operation; `stream_chunks` is recomputed over the redacted body (`_rechunk`).
    """
    guard = _Guard()
    secret_path = _secret_path(exchange.request)
    request = _redact_request(exchange.request, key, guard, secret_path)
    response = exchange.response
    stream_chunks = exchange.stream_chunks
    if response is not None:
        response = _redact_response(response, key, guard, secret_path)
        if stream_chunks and exchange.response is not None:
            stream_chunks = _rechunk(exchange.response.body, response.body, stream_chunks)
    # A bare-origin target keeps the request's path, so it follows the request host's rules.
    target = guard(
        "the answer target",
        REDACTION_FAILED,
        partial(_redact_url, exchange.target, exchange.request.host, key),
    )
    operation = guard(
        "the operation",
        REDACTION_FAILED,
        partial(_redact_operation, exchange.operation, exchange.request, request.path, key),
    )
    flags = exchange.flags
    if guard.failed and Flag.REDACTION_FAILED not in flags:
        flags = (*flags, Flag.REDACTION_FAILED)
    return replace(
        exchange,
        request=request,
        response=response,
        target=target,
        operation=operation,
        flags=flags,
        stream_chunks=stream_chunks,
    )


def redact_request(request: Request, key: bytes) -> Request:
    """Return `request` as `redact_exchange` stores it, for replay matching. Never raises."""
    return _redact_request(request, key, _Guard(), _secret_path(request))


def redact_json(value: JSONValue, key: bytes) -> JSONValue:
    """Return `value` with its secrets replaced, or REDACTION_FAILED. Never raises."""
    failed: JSONValue = REDACTION_FAILED
    return _Guard()("a JSON value", failed, partial(_redact_json, value, key))


def redact_error(error: ErrorInfo | None, key: bytes) -> ErrorInfo | None:
    """Return `error` redacted whole and only then cut, so no half secret survives the cut."""
    if error is None:
        return None
    return ErrorInfo(
        _redact_or_fail(error.type, key), _redact_or_fail(error.message, key)
    ).as_label()


def for_disk(event: Event, key: bytes) -> Event:
    """Return `event` as the store may write it; telemetry holds only a host. Never raises."""
    if isinstance(event, TelemetrySeen):
        return event
    if isinstance(event, ToolCall):
        return replace(
            event,
            args=redact_json(event.args, key),
            result=redact_json(event.result, key),
            error=redact_error(event.error, key),
        )
    return redact_exchange(event, key)


def run_for_disk(record: RunRecord, key: bytes) -> RunRecord:
    """Return `record` with its trigger (name, args, argv) and end error redacted. Never raises."""
    trigger = record.trigger
    if trigger is not None:
        trigger = _redact_trigger(replace(trigger, name=_redact_or_fail(trigger.name, key)), key)
    end = record.end
    if end is not None:
        end = replace(end, error=redact_error(end.error, key))
    return replace(record, trigger=trigger, end=end)


def _redact_trigger(trigger: Trigger, key: bytes) -> Trigger:
    """Return `trigger` with its args or argv redacted as one JSON value (`trace.trigger_args`)."""
    if trigger.argv is None:
        return replace(trigger, args=redact_json(trigger.args, key))
    redacted = redact_json(list(trigger.argv), key)
    if isinstance(redacted, list) and all(isinstance(word, str) for word in redacted):
        return replace(trigger, argv=tuple(str(word) for word in redacted))
    # A command line that could not be redacted is stored as the args that say so.
    return replace(trigger, args=redacted, argv=None)


def _redact_or_fail(text: str, key: bytes) -> str:
    """Return `text` with secret shapes replaced, or REDACTION_FAILED. Never raises."""
    redacted = redact_json(text, key)
    return redacted if isinstance(redacted, str) else REDACTION_FAILED


class _Guard:
    """Run redaction steps, failing each closed, and remember whether any failed."""

    def __init__(self) -> None:
        self.failed = False

    def __call__[T](self, what: str, fallback: T, step: Callable[[], T]) -> T:
        try:
            return step()
        except Exception as exc:
            self.failed = True
            # The type only: a message such as a UnicodeEncodeError's quotes the value.
            logger.warning(
                "irimi: could not redact %s (%s); stored %s instead",
                what,
                type(exc).__name__,
                REDACTION_FAILED,
            )
            return fallback


def _redact_request(
    request: Request, key: bytes, guard: _Guard, secret_path: str | None
) -> Request:
    return replace(
        request,
        path=guard(
            "the request path",
            "/" + REDACTION_FAILED,
            partial(_redact_path, request.host, request.path, key),
        ),
        query=guard("the query", REDACTION_FAILED, partial(_redact_urlencoded, request.query, key)),
        headers=_redact_headers(request.headers, key, guard, secret_path, is_response=False),
        body=guard(
            "the request body",
            REDACTION_FAILED.encode(),
            partial(_redact_body, request.body, _content_type(request.headers), key, secret_path),
        ),
    )


def _redact_response(
    response: Response, key: bytes, guard: _Guard, secret_path: str | None
) -> Response:
    ct = _content_type(response.headers)
    return replace(
        response,
        headers=_redact_headers(response.headers, key, guard, secret_path, is_response=True),
        body=guard(
            "the response body",
            REDACTION_FAILED.encode(),
            partial(_redact_body, response.body, ct, key, secret_path),
        ),
    )


def _rechunk(sent: bytes, stored: bytes, chunks: tuple[int, ...]) -> tuple[int, ...]:
    """Return chunk lengths that split `stored`, the redacted copy of `sent`. Never raises.

    Lines pair up because redaction keeps the line count. A boundary in an unchanged line keeps
    its place; one in a changed line moves to its end. If the lines do not pair up, the stored
    body is one chunk.
    """
    if stored == sent:
        return chunks
    if not stored:
        return ()
    try:
        old, new = _BYTE_LINE.findall(sent), _BYTE_LINE.findall(stored)
        if len(old) != len(new):
            return (len(stored),)
        cuts: set[int] = set()
        line, old_start, new_start, boundary = 0, 0, 0, 0
        for length in chunks[:-1]:
            boundary += length
            while old_start + len(old[line]) < boundary:
                old_start += len(old[line])
                new_start += len(new[line])
                line += 1
            if old[line] == new[line]:
                cuts.add(new_start + boundary - old_start)
            else:
                cuts.add(new_start + len(new[line]))
        edges = [0, *sorted(cut for cut in cuts if 0 < cut < len(stored)), len(stored)]
        return tuple(b - a for a, b in pairwise(edges))
    except Exception:
        return (len(stored),)


def _content_type(headers: Headers) -> str:
    return media_type(header_value(headers, "content-type") or "")


def _redact_headers(
    headers: Headers, key: bytes, guard: _Guard, secret_path: str | None, *, is_response: bool
) -> Headers:
    """Redact each value by `_redact_header`, and scan each name for shapes too."""
    what = "a response header" if is_response else "a request header"
    return tuple(
        (
            guard(f"{what}'s name", REDACTION_FAILED, partial(_redact_text, name, key)),
            guard(
                what,
                REDACTION_FAILED,
                partial(_redact_header, name, v, key, is_response, secret_path),
            ),
        )
        for name, v in headers
    )


def _redact_header(
    name: str, value: str, key: bytes, is_response: bool, secret_path: str | None
) -> str:
    """Hide a credential header whole (`Set-Cookie` on a response); redact a URL value as a URL."""
    if is_credential_header(name) or (is_response and name.strip().lower() == "set-cookie"):
        return _hide(value, key)
    if _ABSOLUTE_URL.match(value):
        value = _redact_url(value, None, key)
    else:
        value = _redact_text(value, key)
    for spelling, hidden in _secret_path_swaps(secret_path, key):
        if spelling in value:
            value = value.replace(spelling, hidden)
    return value


def _is_credential_path_host(host: str) -> bool:
    """Return True for a credential-path host, ignoring a trailing root dot."""
    return host.lower().rstrip(".") in shipped_credential_hosts()


def _secret_path(request: Request) -> str | None:
    """Return a credential-path host's non-root path, a secret wherever it reappears, else None."""
    if _is_credential_path_host(request.host) and request.path.strip("/"):
        return request.path
    return None


def _hidden_path(path: str, key: bytes) -> str:
    return "/" + placeholder(key, path)


def _secret_path_swaps(secret_path: str | None, key: bytes) -> list[tuple[str, str]]:
    """Return `(spelling, replacement)` for `secret_path` as sent and as the shape rule left it."""
    if secret_path is None:
        return []
    hidden = _hidden_path(secret_path, key)
    return [
        (spelling, hidden)
        for spelling in dict.fromkeys((secret_path, _redact_text(secret_path, key)))
    ]


def _redact_path(host: str, path: str, key: bytes) -> str:
    """Redact a path; on a credential-path host (`hooks.slack.com`) the whole path is the secret."""
    if _is_credential_path_host(host):
        return _hidden_path(path, key)
    return _redact_text(path, key)


def _redact_operation(operation: str, sent: Request, stored_path: str, key: bytes) -> str:
    """Redact an operation; an unmatched request's `METHOD /path` name takes the stored path."""
    if operation == f"{sent.method} {sent.path}":
        return f"{sent.method} {stored_path}"
    return _redact_text(operation, key)


def _redact_url(url: str, path_host: str | None, key: bytes) -> str:
    """Redact a URL's path by `path_host`'s rules (default its own), its query and fragment."""
    parts = urlsplit(url)
    host = (parts.hostname or "") if path_host is None else path_host
    path = _redact_path(host, parts.path, key) if parts.path else parts.path
    query = _redact_urlencoded(parts.query, key)
    fragment = _redact_urlencoded(parts.fragment, key)
    if path is not parts.path or query is not parts.query or fragment is not parts.fragment:
        url = parts._replace(path=path, query=query, fragment=fragment).geturl()
    return _redact_text(url, key)


def _redact_urlencoded(text: str, key: bytes) -> str:
    """Redact a query or form body pair by pair, then whole (to catch shapes in names)."""
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
    """Return a form name's last bracket segment: `card[token]` -> `token`, `token[]` -> `token`."""
    segments = [segment for segment in name.replace("]", "").split("[") if segment]
    return segments[-1] if segments else ""


def _redact_body(body: bytes, content_type: str, key: bytes, secret_path: str | None) -> bytes:
    """Return `body` redacted (as is if nothing matched), `secret_path` replaced even in binary."""
    stored = _redact_body_text(body, content_type, key)
    for spelling, hidden in _secret_path_swaps(secret_path, key):
        raw = spelling.encode()
        if raw in stored:
            stored = stored.replace(raw, hidden.encode())
    return stored


def _redact_body_text(body: bytes, content_type: str, key: bytes) -> bytes:
    """Redact a UTF-8 body, keeping a BOM aside; others are stored unscanned (a known limit)."""
    if not body:
        return body
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError:
        return body
    bom = "\ufeff" if text.startswith("\ufeff") else ""
    document = text[len(bom) :]
    redacted = _redact_document(document, content_type, key)
    return body if redacted is document else (bom + redacted).encode()


def _redact_document(text: str, content_type: str, key: bytes) -> str:
    """Redact a body's text as JSON if it parses (whatever its content type), else as a form
    if declared, else line by line.

    JSON refused for another reason (an integer past the digit limit, a repeated key) raises to
    the guard and fails closed: a text scan would miss a secret key's value.
    """
    walked = _redact_json_text(text, key)
    if walked is not None:
        return walked
    if content_type == FORM_CT:
        return _redact_urlencoded(text, key)
    lines = _LINE.findall(text)
    redacted = [_redact_line(line, key) for line in lines]
    if all(new is old for new, old in zip(redacted, lines, strict=True)):
        return text
    return "".join(redacted)


def _redact_line(line: str, key: bytes) -> str:
    """Redact one line: an NDJSON line or SSE `data:` JSON as JSON, any other for shapes."""
    content = line.rstrip("\r\n")
    field = _SSE_DATA if content.startswith(_SSE_DATA) else ""
    value = content[len(field) :]
    document = value.lstrip()
    if document.startswith(("{", "[")):
        walked = _redact_json_text(document, key)
        if walked is document:
            return line
        if walked is not None:
            return field + value[: len(value) - len(document)] + walked + line[len(content) :]
    return _redact_text(line, key)


def _redact_json_text(text: str, key: bytes) -> str | None:
    """Return `text` redacted as JSON: itself if unchanged, else compact JSON; None if not JSON."""
    try:
        document = json.loads(text, object_pairs_hook=_unique_members)
    except json.JSONDecodeError:
        return None
    walked = _redact_json(document, key)
    if walked is document:
        return text
    return json.dumps(walked, separators=(",", ":"), ensure_ascii=False)


def _unique_members(pairs: list[tuple[str, JSONValue]]) -> dict[str, JSONValue]:
    """Return a JSON object's members; raise ValueError on a repeated key the walk would miss."""
    members = dict(pairs)
    if len(members) != len(pairs):
        raise ValueError("a JSON object repeats a key")
    return members


def _redact_json(value: JSONValue, key: bytes) -> JSONValue:
    """Return `value` itself if nothing matched, else a redacted copy; TypeError if not JSON."""
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
    if value is None or isinstance(value, bool | int | float):
        return value
    # A tuple would pass unscanned yet `json.dumps` would write it as an array.
    raise TypeError(f"{type(value).__name__} is not JSON")


def _redact_member(name: str, value: JSONValue, key: bytes) -> JSONValue:
    """Hide a secret key's value whole (non-strings as sorted-key JSON); keep `null`."""
    if not _is_secret_key(name):
        return _redact_json(value, key)
    if value is None:
        return value
    return _hide(value if isinstance(value, str) else json.dumps(value, sort_keys=True), key)


def _is_secret_key(name: str) -> bool:
    return name.lower() in SECRET_KEYS


def _hide(value: str, key: bytes) -> str:
    """Return a secret's placeholder; an empty value stays empty so it still reads as unsent."""
    return value if value == "" else placeholder(key, value)


def _redact_text(text: str, key: bytes) -> str:
    """Return `text` itself when it holds no secret shape, else a copy with each replaced."""
    redacted, count = SECRET_SHAPE.subn(lambda m: placeholder(key, m.group(0)), text)
    return redacted if count else text
