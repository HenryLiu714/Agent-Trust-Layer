"""The HTTP opener irimi's own requests use: no proxy, no redirects, only the host they name."""

import urllib.request
from typing import Any


class _NoRedirects(urllib.request.HTTPRedirectHandler):
    """Follow no redirect, so a 3xx reaches the caller as an HTTPError."""

    def redirect_request(self, *args: Any, **kwargs: Any) -> None:
        return None


def no_redirect_opener(*handlers: urllib.request.BaseHandler) -> urllib.request.OpenerDirector:
    """Return an opener that ignores proxy variables and follows no redirect, plus `handlers`."""
    return urllib.request.build_opener(urllib.request.ProxyHandler({}), *handlers, _NoRedirects())
