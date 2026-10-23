"""The one HTTP opener irimi's own requests use: no proxy, no redirects."""

import urllib.request
from typing import Any


class _NoRedirects(urllib.request.HTTPRedirectHandler):
    """Never follows a redirect, so a 3xx is an HTTPError the caller reads as the answer it is."""

    def redirect_request(self, *args: Any, **kwargs: Any) -> None:
        return None


def no_redirect_opener(*handlers: urllib.request.BaseHandler) -> urllib.request.OpenerDirector:
    """An opener that reads no proxy variable and follows no redirect, plus `handlers`.

    urllib's default opener sends a request to an exported `HTTP_PROXY` and follows a 301, 302,
    303, 307 or 308 to wherever it points. irimi's precondition reads and the SDK's control posts
    must reach the host they name and no other.
    """
    return urllib.request.build_opener(urllib.request.ProxyHandler({}), *handlers, _NoRedirects())
