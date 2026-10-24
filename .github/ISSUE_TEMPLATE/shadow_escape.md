---
name: A write reached the real service
about: Under irimi shadow or serve, a write happened for real
labels: bug, shadow-escape
---

> **Remove real customer data before pasting**, and never paste a live API key. If the escape
> exposes credentials or another user's data, report it privately instead (see SECURITY.md).
>
> Traffic that never goes through the proxy (a client that ignores `HTTPS_PROXY`, a database,
> gRPC) is a known limit, listed in the README under "What irimi does not catch". Report it here
> only if irimi saw the request.

## The write

<!-- Service, method and path, e.g. `POST api.stripe.com/v1/refunds`. -->

## How you know it reached the service

<!-- E.g. the object exists in the service's dashboard, or the response had no `Irimi-Answered-By` header. -->

## irimi's output

<!-- The exchange line for the request (`irimi shadow` prints one per request) and the summary, with real data removed. -->

```
```

## How to reproduce

<!-- The exact command, and how the agent makes the request: which client library, proxy or base URL. -->

## Environment

- irimi version (`irimi --version`):
- OS and version:
- Python version:
- Mode: `shadow`, `serve` or `replay`:
