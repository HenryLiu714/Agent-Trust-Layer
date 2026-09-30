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
raise. Layer 2: it imports `exchange`, `paths`, `ca` (its file modes), `bodies`, `servicemap`
and `trace`.
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

from irimi import paths
from irimi.bodies import FORM_CT
from irimi.ca import DIR_MODE, KEY_MODE
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
#
# Unless that letter or digit ends an escape. Text stored raw spells a separator as one: a JSON
# string inside an SSE line as `\n` or `\u0020`, a path, a header or a form field's name as `%20`,
# `%3D` or `%2F`. Each ends in a letter or a digit, so `key:\nsk_live_…` and
# `/Bearer%20sk_live_…` were stored whole (#69).
_BOUNDARY = r"(?:(?<![A-Za-z0-9])|(?<=\\[bfnrt])|(?<=\\u[0-9A-Fa-f]{4})|(?<=%[0-9A-Fa-f]{2}))"
_SECRET_SHAPE = re.compile(_BOUNDARY + "(?:" + "|".join(SECRET_PATTERNS) + ")")

# A header value that is an absolute URL: `Location` hands an OAuth token back in its query or
# fragment, and `Referer` repeats a page's, with no shape either would be caught by (#69).
_ABSOLUTE_URL = re.compile(r"\s*https?://", re.IGNORECASE)

# An SSE field whose value may be one JSON document (#69).
_SSE_DATA = "data:"

# A line of a text body with its ending: CRLF, LF or a lone CR, the three SSE ends a line at. Split
# at LF alone, a CR-only stream was one line that was not JSON, and kept `data: {"token": …}`'s
# token (#69). A JSON string holds neither CR nor LF raw, so no JSON line is split by this.
_LINE = re.compile(r"[^\r\n]*(?:\r\n|\r|\n)|[^\r\n]+")

# `_LINE` over bytes, for `_rechunk`: a chunk length counts bytes, and a line of text does not.
# Built from `_LINE` so the two cannot disagree on where a line ends, and they split alike: in
# UTF-8, CR and LF are never a byte of a longer character (#71).
_BYTE_LINE = re.compile(_LINE.pattern.encode())


def complete_lines(body: bytes) -> bytes:
    """`body` up to the end of its last line, where `_LINE` ends one: CRLF, LF or a lone CR. What
    the engine keeps of a stream that is not whole, so that every line of it is one redaction can
    read (#71). Never raises."""
    return body[: max(body.rfind(b"\n"), body.rfind(b"\r")) + 1]


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
    would do the same to every recording already on disk. So is a path that does not lead to a
    regular file - a directory, a dangling symlink - rather than a bare OSError (#69).

    A symlink to a regular file is followed: a key shared between machines is mounted that way (a
    Kubernetes Secret volume is a tree of symlinks), and refusing it would leave the partner no
    way to give two installs one key. A missing home is created with `ca.DIR_MODE`, as the
    directory that holds the CA's key is; an existing one keeps its mode, since it holds more than
    the key and the key's own KEY_MODE is what protects it.
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

    The key is written to a file of its own, created with O_CREAT | O_EXCL and mode 0600, and then
    hard-linked to `path`. `os.link` never replaces an existing file, so the first process to link
    wins, and `path` never exists half-written: a process that loses the race reads the winner's
    whole key, where creating `path` directly could hand it an empty file.

    There is no fallback for a filesystem without hard links: the OSError is raised. `os.rename`
    replaces, so a loser would overwrite a key the winner had already hashed with.
    """
    path.parent.mkdir(mode=DIR_MODE, parents=True, exist_ok=True)
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

    The request's path, query, headers and body, the response's headers and body, the answer
    target and the operation are redacted. Every other field is copied with `dataclasses.replace`,
    so a field added to Exchange later survives without a change here. A part that could not be
    redacted is stored as REDACTION_FAILED and the copy gains REDACTION_FAILED_FLAG.

    `stream_chunks` is the one field that follows the body: a placeholder is not as long as its
    secret, so the chunk lengths are recomputed over the redacted body (`_rechunk`, #71).
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
    # A bare-origin target keeps the request's path (`delegation.target_url`), so it is redacted
    # by the request's host's rules: a webhook's secret path would otherwise reach disk here.
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
    if guard.failed and REDACTION_FAILED_FLAG not in flags:
        flags = (*flags, REDACTION_FAILED_FLAG)
    return replace(
        exchange,
        request=request,
        response=response,
        target=target,
        operation=operation,
        flags=flags,
        stream_chunks=stream_chunks,
    )


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
    """The chunk lengths of `stored`, the redacted copy of the streamed body `sent` that `chunks`
    split. Always a split of `stored`: positive lengths summing to `len(stored)`. Never raises.

    Redaction reads a stream line by line (`_redact_line`) and never adds or removes a line, so the
    two bodies are compared line for line. A chunk boundary in a line redaction left alone keeps
    its place in that line. One inside a line it changed moves to the end of that line: a JSON
    `data:` line is re-serialized whole, so no offset inside it maps to the new one. Two boundaries
    that land together make one chunk. When the lines do not pair up - the body was one JSON
    document, or could not be redacted at all - the stored body is one chunk.
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
    """Each value by `_redact_header`, and each name scanned for shapes, because a name is text
    the store writes too (#69)."""
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
    """A credential header's whole value is hidden; `Set-Cookie` is one on a response only. A
    value that is an absolute URL is redacted as one, by its own host's rules."""
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
    """Compared without a root dot: `hooks.slack.com.` is the same host (#69)."""
    return host.lower().rstrip(".") in CREDENTIAL_PATH_HOSTS


def _secret_path(request: Request) -> str | None:
    """A credential-path host's path, which is a secret wherever else the exchange carries it too:
    irimi's own 502 for an unreachable answer target names the target's URL, and a bare-origin
    target's URL ends in this path (#69). None on any other host, and for `/`, which is every
    URL's path and hides nothing."""
    if _is_credential_path_host(request.host) and request.path.strip("/"):
        return request.path
    return None


def _hidden_path(path: str, key: bytes) -> str:
    return "/" + placeholder(key, path)


def _secret_path_swaps(secret_path: str | None, key: bytes) -> list[tuple[str, str]]:
    """Each spelling of `secret_path` a header or a body can hold once the other rules have run,
    and what it is stored as. The shape rule runs first, so a path holding `xoxb-…` is carried as
    that rule left it, and replacing the path as sent alone kept its team and bot ids (#69)."""
    if secret_path is None:
        return []
    hidden = _hidden_path(secret_path, key)
    return [
        (spelling, hidden)
        for spelling in dict.fromkeys((secret_path, _redact_text(secret_path, key)))
    ]


def _redact_path(host: str, path: str, key: bytes) -> str:
    """On a credential-path host (`hooks.slack.com`) the path IS the secret, so all of it goes."""
    if _is_credential_path_host(host):
        return _hidden_path(path, key)
    return _redact_text(path, key)


def _redact_operation(operation: str, sent: Request, stored_path: str, key: bytes) -> str:
    """A request no route matched is named `METHOD /path` (`pipeline.classify`), so its operation
    is the stored path's: slack.yaml lists no route for `hooks.slack.com/workflows/...` (#69). A
    route's own name is scanned for shapes like any other text."""
    if operation == f"{sent.method} {sent.path}":
        return f"{sent.method} {stored_path}"
    return _redact_text(operation, key)


def _redact_url(url: str, path_host: str | None, key: bytes) -> str:
    """A URL's path by `path_host`'s rules (the URL's own host when None), its query and its
    fragment pair by pair, then the whole of it for shapes. `url` itself when nothing matched."""
    parts = urlsplit(url)
    host = (parts.hostname or "") if path_host is None else path_host
    path = _redact_path(host, parts.path, key) if parts.path else parts.path
    query = _redact_urlencoded(parts.query, key)
    fragment = _redact_urlencoded(parts.fragment, key)
    if path is not parts.path or query is not parts.query or fragment is not parts.fragment:
        url = parts._replace(path=path, query=query, fragment=fragment).geturl()
    return _redact_text(url, key)


def _redact_urlencoded(text: str, key: bytes) -> str:
    """A query string or a form body, pair by pair, then scanned whole for secret shapes. A pair
    nothing matched keeps its bytes, and `text` itself comes back when nothing matched at all.

    The whole-text scan is what catches a shape in a field's NAME.
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
    """`card[token]` -> `token`; `a[b][c]` -> `c`; `token` -> `token`. An empty `[]` is a list's
    and names nothing, so `token[]` -> `token` (#69)."""
    segments = [segment for segment in name.replace("]", "").split("[") if segment]
    return segments[-1] if segments else ""


def _redact_body(body: bytes, content_type: str, key: bytes, secret_path: str | None) -> bytes:
    """`body` itself unless a rule changed something, so a body with nothing to hide is stored
    byte for byte: its key order, its whitespace, its escapes. `secret_path` is replaced wherever
    it appears, even in a body that is not UTF-8.
    """
    stored = _redact_body_text(body, content_type, key)
    for spelling, hidden in _secret_path_swaps(secret_path, key):
        raw = spelling.encode()
        if raw in stored:
            stored = stored.replace(raw, hidden.encode())
    return stored


def _redact_body_text(body: bytes, content_type: str, key: bytes) -> bytes:
    """A body by the rules that read it. One that is not UTF-8 is stored unscanned - a known
    limitation, in `docs/trace-format.md`. A leading byte-order mark is set aside and put back,
    because `json.loads` refuses one as not JSON (#69).
    """
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
    """A body's text: walked as JSON when it is one JSON document, whatever its content type
    claims - `{"password": "…"}` read as a form is one pair whose name is the whole document, so
    no secret key is ever seen (#69). Otherwise a form when its content type says so, and
    otherwise line by line (`_redact_line`).

    JSON that `json.loads` refuses for a reason other than not being JSON (an integer past its
    digit limit) raises to the guard and fails closed, because a text scan would miss a secret
    key's value. So does an object with a repeated key: `loads` keeps the last value, and
    `{"token": "…", "token": null}` would come back byte for byte.
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
    """One line of a body that is not one JSON document. A line that is one (NDJSON), or an SSE
    `data:` field whose value is one, is walked as JSON; a text scan alone kept `data: {"token":
    "…"}`'s token (#69). Any other line is scanned for shapes.
    """
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
    """`text` walked as one JSON document: `text` itself when nothing in it matched, compact JSON
    when something did, and None when it is not JSON at all."""
    try:
        document = json.loads(text, object_pairs_hook=_unique_members)
    except json.JSONDecodeError:
        return None
    walked = _redact_json(document, key)
    if walked is document:
        return text
    return json.dumps(walked, separators=(",", ":"), ensure_ascii=False)


def _unique_members(pairs: list[tuple[str, JSONValue]]) -> dict[str, JSONValue]:
    """A JSON object's members as a dict, refusing a repeated key, whose earlier values the walk
    would never see (#69)."""
    members = dict(pairs)
    if len(members) != len(pairs):
        raise ValueError("a JSON object repeats a key")
    return members


def _redact_json(value: JSONValue, key: bytes) -> JSONValue:
    """`value` itself when nothing in it matched, else a copy; the identity is the signal. Raises
    TypeError on a value that is not JSON."""
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
    # Not JSON, so nothing here can say what it holds: a tuple's secret would still reach disk,
    # since `json.dumps` writes a tuple as an array (#69).
    raise TypeError(f"{type(value).__name__} is not JSON")


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
