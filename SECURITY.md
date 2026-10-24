# Security

irimi generates a local certificate authority, decrypts the HTTPS traffic of the processes it
launches, and stores what those processes read. This page says how to report a problem, which
versions get fixes, what irimi protects and what it does not, and how to remove it.
[`docs/security.md`](https://github.com/HenryLiu714/Irimi/blob/main/docs/security.md) has the full
threat model, the redaction rules and the rules every run keeps.

## Reporting a vulnerability

Report it privately, through GitHub's
[private vulnerability reporting](https://github.com/HenryLiu714/Irimi/security/advisories/new)
on this repository, or by email:

security contact: <owner to fill>

Please do not open a public issue for a vulnerability. Include the irimi version
(`irimi --version`), your OS and Python, and the steps to reproduce. Remove real customer data and
credentials from anything you send.

In scope, among others:

- a write that reaches the real service under `irimi shadow` or `irimi serve` through the proxy;
- a credential that reaches disk unredacted;
- the CA key, or a run's stored data, readable by another user;
- irimi forwarding a request to a host it should refuse.

A client that never sends its traffic through the proxy is a known limit, not a vulnerability
(see below). A write that escapes shadow mode without a security impact can go in a public
[shadow-escape issue](https://github.com/HenryLiu714/Irimi/issues/new?template=shadow_escape.md).

## Supported versions

irimi is 0.x. Fixes go into the latest release only; upgrade to get them.

| Version | Supported |
| --- | --- |
| latest 0.x release | yes |
| older releases | no |

## Threat model, in short

- **The CA.** `irimi init` generates an RSA-2048 CA on your machine, valid for 825 days. Its key is
  `$IRIMI_HOME/ca/ca.key` (mode 0600, in a 0700 directory); `$IRIMI_HOME/mitm/` holds a copy for
  the proxy. irimi never adds the CA to an OS or browser trust store. Only the processes irimi
  launches trust it, through environment variables such as `SSL_CERT_FILE`. Anyone who holds
  `ca.key` can impersonate any host to a process that trusts `ca.pem`, so keep the key private
  and never copy the `mitm/` directory. The CA cannot be limited to certain hosts, because irimi
  must issue certificates for whatever host the agent calls. `irimi init --force` replaces it,
  and irimi warns when it expires within 30 days.
- **The listener.** irimi listens on loopback by default. Its control endpoint, where the SDK
  reports runs, has no authentication. `--bind` widens both, so bind wider only on a network that
  holds the agent and nothing else.
- **The store.** Each run is stored under `$IRIMI_HOME/store` (or `--store`), in files only your
  user can read. Credentials are redacted before anything reaches disk: they become keyed HMAC
  placeholders under a per-install key, `$IRIMI_HOME/redact.key`. Response bodies are not
  redacted, so a stored run holds the production data the agent read, customer names and emails
  included. irimi keeps runs until you delete them with `irimi runs rm` or
  `irimi runs prune --older-than 30d`.
- **No telemetry.** irimi sends nothing of its own anywhere. It forwards the agent's reads, LLM
  calls and telemetry, and the read a precondition check makes.
- **Known limits.** Only traffic sent through the proxy is shadowed. A client that ignores
  `HTTPS_PROXY` talks to the real service, and database writes, files, gRPC and WebSockets happen
  for real unless the agent wraps them in `@sdk.write_tool`. A client that does not trust irimi's
  CA fails TLS and sends nothing. The
  [known bypasses](https://github.com/HenryLiu714/Irimi/blob/main/docs/security.md#known-bypasses)
  are each pinned by the sample workflow `w11_leaky_agent`.

## Removing irimi

1. Uninstall it: `uv tool uninstall irimi`, or `pipx uninstall irimi`.
2. Delete its state, which holds the CA, the proxy's copy of the key, the redaction key and every
   stored run: `rm -rf ~/.irimi`, or your `$IRIMI_HOME`, and any `--store` directory you used.
3. If you ran the sidecar example, remove its volumes: `docker compose down -v` in
   `examples/sidecar`.
4. If you copied `ca.pem` into a container image or added it to a trust store, remove it there.
   irimi never does this itself.

To rotate the CA without removing irimi, run `irimi init --force` and give the new `ca.pem` to
whatever trusted the old one.
