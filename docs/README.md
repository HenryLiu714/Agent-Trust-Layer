# irimi documentation

For anyone going past the project README: users running irimi on their own agent, security
reviewers deciding whether it may sit in front of production, authors of tools and SDKs that talk
to it, and contributors. Start with the concepts; each other page stands on its own.

## Using irimi

- [Concepts](concepts.md): the terms every other page uses, one meaning each.
- [How it works](how-it-works.md): a request through irimi, and what `shadow`, `serve` and
  `replay` each do.
- [Reading the summary](reading-the-summary.md): every line and status irimi prints.
- [The `irimi` command](cli.md): every command, flag and exit code.
- [The SDK](sdk.md): marking runs and the tools the proxy cannot see.
- [Serve mode](serve-mode.md): irimi as a sidecar to an agent it does not launch.
- [Record, compare and replay](replay.md): what a change to the agent would do differently.
- [Limitations](limitations.md): what irimi does not do, or does only in part.

## Reference

- [Security model](security.md): trust boundaries, the five invariants, redaction, known bypasses.
- [Service maps](service-maps.md): the map schema, key by key, and targets.
- [Trace format](trace-format.md): the specification of what irimi stores.
- [Control protocol](control-protocol.md): the wire format an SDK speaks to irimi.

## Contributing

- [Architecture](architecture.md): packages, layers, threads and seams.
- [Adding a service](adding-a-service.md): a map, a `Service` spec, and the tests to extend.
