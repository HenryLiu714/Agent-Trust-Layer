---
name: Bug report
about: irimi did something wrong, crashed, or printed something misleading
labels: bug
---

> **Remove real customer data before pasting.** A summary, a `runs show` or a stored run holds the
> data your agent read. Credentials are redacted on disk, but names, emails, amounts and ids are
> not. Replace them with placeholders, and never paste a live API key.
>
> A write that reached the real service has its own template ("A write reached the real
> service"). A security problem goes to SECURITY.md, not here.

## What happened

<!-- One or two sentences. -->

## What you expected

## How to reproduce

<!-- The exact command, and the smallest agent or curl that shows the problem. -->

```
irimi shadow -- ...
```

## Output

<!-- irimi's summary and any error, with real data removed. -->

```
```

## Environment

- irimi version (`irimi --version`):
- OS and version:
- Python version (`python3 --version`), and how irimi was installed (uv tool, pipx, a clone):
- Mode: `shadow`, `serve` or `replay`:
