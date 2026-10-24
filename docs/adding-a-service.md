# Adding a service

For contributors teaching irimi a new service, or more about one it knows. A service can be
supported at two depths: a map alone, which needs no Python, or a map plus a `Service` spec, which
adds fixtures, the overlay, precondition checks and idempotency. This page says where each piece
lives, how irimi finds it, and which tests and sample workflows to extend.

## A map alone

Write `src/irimi/maps/<service>.yaml` to the schema in [service-maps.md](service-maps.md). That
is enough for irimi to classify the service's routes, forward its reads and answer its writes with
an echo: the request's fields reflected back, with the ids the route's `ids:` names minted.

- List every read route with its method. An unlisted `GET` is forwarded as a read anyway, and an
  unlisted write is faked as `unknown`; listing it names the operation in the summary.
- Give each write a `human:` sentence, so the summary says what the write would have done.
- A telemetry vendor lists its intake routes as `telemetry`; never a `default_kind`.
- Shipped maps never set a `target:`.

Then extend:

- `tests/test_servicemap.py`, which pins every shipped map's hosts, route counts and `fires:`
  lists, and that the map is inside a built wheel;
- `tests/test_classify.py`, which pins how each shipped route classifies.

## A `Service` spec

What a map cannot say is Python, in one package per service: `src/irimi/services/<service>/`.

```
services/
  model.py           the Service spec and the plain data its functions take
  generic.py         the echo and fixture bodies every service gets by default
  __init__.py        REGISTRY, and the answer path that consults it
  stripe/            one service:
    __init__.py        its SERVICE spec (and idempotency rules)
    answers.py         how its faked writes are answered
    effects.py         what its faked writes do to later live reads
    preconditions.py   its precondition checks
```

A service's `__init__.py` builds one frozen `services.model.Service`:

| Field | What it holds | Default |
| --- | --- | --- |
| `name` | the map's `service:` name | required |
| `operations` | every map `operation:` this code keys on | none |
| `answer` | how faked writes are answered: a `body(request, route, fixture, clock)` function, and literal non-JSON bodies by operation | the generic echo and fixture |
| `observe_read` | learns from each live read's body (Slack learns the newest `ts`, so a faked message sorts after it) | nothing |
| `effects` | applies the run's faked writes to a live read's document, returning an `Applied` | no overlay |
| `rewrite` | translates a read before it is forwarded (a list cursor naming a minted id) | none |
| `preconditions` | checks by `precondition:` name: the check read to make (`Probe`), and the verdict on its document | none |
| `idempotency` | the retry header, the service's conflict error, and a header marking a retried answer | no retry handling |
| `scope_headers` | headers that partition the service's state (Stripe's `Stripe-Account`); a write applies only to reads with the same values | one scope |
| `probe_headers` | credential headers irimi's check read copies from the write | `authorization` |

Add the spec to `REGISTRY` in `services/__init__.py`, and place the new package in the layer table
of `tests/safety/test_import_boundary.py` if it is a new top-level package (a service package sits
inside `services/`, so it needs no entry).

### Fixtures

A route's `fixture:` names an object in `src/irimi/fixtures/<service>.json`: a whole response, so
the agent reads `status` and `currency` rather than `None`. Vendor it from the service's own mock
server where one exists, otherwise write it against the service's published docs, and say which in
the file's `_source` entry. Use obviously synthetic ids that keep the object's prefix.

### Rules the code holds you to

- **Names must agree.** `services.check` runs when the maps load and refuses, by file and route, a
  registered service with no map, an operation the spec keys on that no route names, a
  `precondition:` the spec has no check for, and a `fixture:` the fixtures file does not hold. A
  rename on either side cannot silently switch an effect off.
- **Effects are pure.** They take plain data (`Read`, `Write`), no clock, I/O or module state, so a
  replay reuses them. What they cannot apply whole they leave as the service sent it, marked
  `partial`. They never add a field the live object lacks.
- **Nothing on the answer path raises** (the fail-closed hook rule). A check irimi could not finish
  is `not_evaluable`, and the write is still faked.

## Tests and workflows to extend

- Unit tests: `tests/test_services_<service>.py` (effects and answers),
  `tests/test_services_preconditions.py`, `tests/test_services_answers.py`, `tests/test_fixture.py`
  (every fixture id has its object's prefix) and `tests/test_overlay.py`.
- End to end: every feature is driven through the real `irimi shadow` in the sample workflows under
  `tests/e2e/workflows/`. Extend the scenario and pins of the workflow that exercises the nearest
  behaviour; for a new service, add a workflow by copying W2 (`w02_nightly_reconcile`), as the
  corpus README says. Its fake service goes in `tests/e2e/workflows/harness/services.py`. The five
  invariants the corpus checks must keep holding.

Run `make check` before opening the pull request.
