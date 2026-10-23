"""Test maps, and loading them the way the composition root does."""

from pathlib import Path

import pytest
import yaml

import irimi
from irimi import servicemaps, services
from irimi.config import paths
from irimi.servicemaps import MapError

# The upstream in these tests lives on 127.0.0.1, so a map claiming that host makes its two routes
# classified ones. Hosts carry no port, so the upstream's random port does not matter.
DEMO_MAP = """
version: 1
service: demo
hosts:
  - 127.0.0.1
routes:
  - match:
      method: GET
      path: /hello
    operation: things.list
    kind: read
    human: list things
  - match:
      method: POST
      path: /things
    operation: things.create
    kind: write
    human: create a thing
    fires:
      - thing.created
"""

# A map claiming the loopback upstream as `slack`, so a read through it is a Slack read. The
# service name is what the `ts` watermark keys on, not the host or the port.
SLACK_MAP = """
version: 1
service: slack
hosts:
  - 127.0.0.1
routes:
  - match:
      method: GET
      path: /slack-history
    operation: conversations.history
    kind: read
    human: read the history
"""


def map_index(tmp_path, monkeypatch, doc=DEMO_MAP):
    """A MapIndex holding `doc` alone. $IRIMI_HOME is pointed somewhere empty first: the loader
    merges `$IRIMI_HOME/maps.yaml` when it exists, and a developer may have a real one."""
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path / "maps-home"))
    maps_dir = tmp_path / "maps"
    maps_dir.mkdir(exist_ok=True)
    (maps_dir / "demo.yaml").write_text(doc)
    return servicemaps.load(cwd=tmp_path, maps_dir=maps_dir)


STREAM_MAP = """
version: 1
service: llmhost
hosts:
  - 127.0.0.1
routes:
  - match:
      method: POST
      path: /v1/chat/completions
    operation: chat.completions.create
    kind: llm
    human: chat completion
"""

READ_STREAM_MAP = STREAM_MAP.replace("kind: llm", "kind: read").replace(
    "method: POST", "method: GET"
)


def targeted_index(tmp_path, monkeypatch, targets=(), target_reads=()):
    """A MapIndex over DEMO_MAP with `--target` style flags already applied."""
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path / "maps-home"))
    maps_dir = tmp_path / "maps"
    maps_dir.mkdir(exist_ok=True)
    (maps_dir / "demo.yaml").write_text(DEMO_MAP)
    return servicemaps.load(
        cwd=tmp_path, maps_dir=maps_dir, targets=targets, target_reads=target_reads
    )


# A map claiming the loopback upstream as `stripe`, so the real `ServiceOverlay` applies Stripe's
# effects table to reads through it. `fixture: refund` resolves against the shipped Stripe
# fixtures, which are keyed by the service name and not by the host.
STRIPE_MAP = """
version: 1
service: stripe
hosts:
  - 127.0.0.1
routes:
  - match:
      method: POST
      path: /v1/refunds
    operation: refunds.create
    kind: write
    human: refund {amount:money} on {charge}
    fixture: refund
    ids:
      id: re_
      balance_transaction: txn_
    fires:
      - refund.created
      - charge.refunded
  - match:
      method: GET
      path: /v1/refunds
    operation: refunds.list
    kind: read
    human: list refunds
  - match:
      method: GET
      path: /v1/refunds/{refund}
    operation: refunds.retrieve
    kind: read
    human: get refund {refund}
  - match:
      method: GET
      path: /v1/charges
    operation: charges.list
    kind: read
    human: list charges
  - match:
      method: GET
      path: /v1/charges/{charge}
    operation: charges.retrieve
    kind: read
    human: get charge {charge}
  - match:
      method: POST
      path: /v1/customers/{customer}
    operation: customers.update
    kind: write
    human: update customer {customer}
    fixture: customer
    ids:
      id: cus_
  - match:
      method: GET
      path: /v1/customers/{customer}
    operation: customers.retrieve
    kind: read
    human: get customer {customer}
  - match:
      method: POST
      path: /v1/payment_intents/{payment_intent}/cancel
    operation: payment_intents.cancel
    kind: write
    human: cancel {payment_intent}
    fixture: payment_intent
    ids:
      id: pi_
  - match:
      method: GET
      path: /v1/payment_intents/{payment_intent}
    operation: payment_intents.retrieve
    kind: read
    human: get payment intent {payment_intent}
"""


# The loopback upstream claimed as `slack`, with the shipped map's two routes that matter here.
# `style: rpc` because a Slack read is a POST, as it is in `maps/slack.yaml`; `fixture:
# message` resolves against the shipped Slack fixtures, keyed by the service name.
SLACK_OVERLAY_MAP = """
version: 1
service: slack
style: rpc
hosts:
  - 127.0.0.1
routes:
  - match:
      method: POST
      path: /api/chat.postMessage
    operation: chat.postMessage
    kind: write
    human: post to {channel}
    fixture: message
  - match:
      method: POST
      path: /api/conversations.history
    operation: conversations.history
    kind: read
    human: read the {channel} history

  - match:
      method: POST
      path: /api/conversations.replies
    operation: conversations.replies
    kind: read
    human: read a thread in {channel}
"""

# STRIPE_MAP with the shipped map's `precondition:` on `refunds.create`. Its own copy, so the
# overlay tests in `tests/engine/test_overlay_stripe.py` keep their meaning and issue no probes.
PRECONDITION_STRIPE_MAP = STRIPE_MAP.replace(
    "    fixture: refund\n", "    fixture: refund\n    precondition: charge_refundable\n"
)

# SLACK_OVERLAY_MAP with the shipped map's `precondition:` on `chat.postMessage`, and the
# `conversations.info` read the probe has to classify as - the policy refuses to issue anything
# the maps do not call a read.
SLACK_PRECONDITION_MAP = SLACK_OVERLAY_MAP.replace(
    "    fixture: message\n", "    fixture: message\n    precondition: channel_postable\n"
) + (
    "  - match:\n"
    "      method: POST\n"
    "      path: /api/conversations.info\n"
    "    operation: conversations.info\n"
    "    kind: read\n"
    "    human: look up {channel}\n"
)


REPLAY_STRIPE_MAP = """
version: 1
service: stripe
hosts:
  - 127.0.0.1
routes:
  - match:
      method: GET
      path: /v1/charges/{charge}
    operation: charges.retrieve
    kind: read
    human: get charge {charge}
  - match:
      method: POST
      path: /v1/refunds
    operation: refunds.create
    kind: write
    human: refund {amount:money} on {charge}
    fixture: refund
    precondition: charge_refundable
    fires:
      - refund.created
    ids:
      id: re_
  - match:
      method: GET
      path: /v1/refunds
    operation: refunds.list
    kind: read
    human: list refunds
  - match:
      method: GET
      path: /v1/refunds/{refund}
    operation: refunds.retrieve
    kind: read
    human: get refund {refund}
"""

REPLAY_LLM_MAP = """
version: 1
service: anthropic
hosts:
  - localhost
routes:
  - match:
      method: POST
      path: /v1/messages
    operation: messages.create
    kind: llm
    human: message with {model}
    compare_ignores: [metadata, user, stream, stream_options, prompt_cache_key]
"""


SHIPPED_MAPS_DIR = Path(irimi.__file__).parent / "maps"


def complete_for_check(maps_dir: Path) -> None:
    """Add to `maps_dir` each service irimi's Python keys on that no test map defines (the
    shipped file), and to each test map the shipped routes of the operations it leaves out.

    The added routes are ones the test's agent never calls, so they change no answer it gets.
    """
    shipped: dict[str, tuple[Path, dict]] = {}
    for path in sorted(SHIPPED_MAPS_DIR.glob("*.yaml")):
        for doc in yaml.safe_load_all(path.read_text()):
            if doc is not None:
                shipped[doc["service"]] = (path, doc)
    defined: dict[str, tuple[Path, dict]] = {}
    for path in sorted(maps_dir.glob("*.yaml")):
        docs = [d for d in yaml.safe_load_all(path.read_text()) if d is not None]
        if len(docs) == 1:
            defined[docs[0]["service"]] = (path, docs[0])
    for service, spec in services.REGISTRY.items():
        operations = spec.operations
        source, shipped_doc = shipped[service]
        if service not in defined:
            (maps_dir / f"shipped-{source.name}").write_text(yaml.safe_dump(shipped_doc))
            continue
        path, doc = defined[service]
        named = {route["operation"] for route in doc["routes"]}
        missing = [r for r in shipped_doc["routes"] if r["operation"] in operations - named]
        if missing:
            doc["routes"].extend(missing)
            path.write_text(yaml.safe_dump(doc, sort_keys=False))


def index_from(tmp_path, monkeypatch, *docs: str):
    """A MapIndex from `docs`, or from the shipped maps when none are given.

    Both override locations are pointed at empty places: the loader reads `./irimi.maps.yaml` and
    then `$IRIMI_HOME/maps.yaml`, so a developer with a real overrides file would otherwise see a
    different index, or a MapError.
    """
    from irimi import servicemaps
    from irimi.config import paths

    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path / "ambient-home"))
    if not docs:
        return servicemaps.load(cwd=tmp_path, maps_dir=servicemaps.loader.shipped_dir())
    maps_dir = tmp_path / "maps"
    maps_dir.mkdir(exist_ok=True)
    for i, doc in enumerate(docs):
        (maps_dir / f"{i}.yaml").write_text(doc)
    return servicemaps.load(cwd=tmp_path, maps_dir=maps_dir)


# A complete, valid one-service document. Tests mutate a copy of this to make one thing wrong.
GOOD_MAP = """
version: 1
service: demo
style: rest
hosts:
  - demo.example
  - files.demo.example
routes:
  - match:
      method: GET
      path: /v1/things
    operation: things.list
    kind: read
    human: list things
  - match:
      method: GET
      path: /v1/things/{thing}
    operation: things.retrieve
    kind: read
  - match:
      method: POST
      path: /v1/things
    operation: things.create
    kind: write
    human: create thing {name}
    fixture: thing
    ids:
      id: th_
    compare_ignores:
      - nonce
  - match:
      method: POST
      path: /v1/search
    operation: things.search
    kind: read
    live_because: search persists nothing; Stripe's own spec calls it a POST read
"""


def write_maps(tmp_path, *docs: str):
    """Write each document as its own <n>.yaml in a fresh maps directory and return it."""
    directory = tmp_path / "maps"
    directory.mkdir(exist_ok=True)
    for index, doc in enumerate(docs):
        (directory / f"{index}.yaml").write_text(doc)
    return directory


def load_docs(tmp_path, *docs: str, allow=frozenset(), override: str | None = None):
    """Load `docs` as the shipped maps, with an optional ./irimi.maps.yaml overrides file."""
    home = tmp_path / "home"
    home.mkdir(exist_ok=True)
    cwd = tmp_path / "cwd"
    cwd.mkdir(exist_ok=True)
    if override is not None:
        (cwd / servicemaps.loader.CWD_OVERRIDE_NAME).write_text(override)
    return servicemaps.load(
        allow_target_hosts=allow, cwd=cwd, maps_dir=write_maps(tmp_path, *(docs or (GOOD_MAP,)))
    )


def assert_refused(tmp_path, doc: str, message: str, **kwargs):
    """Assert loading `doc` raises MapError whose text contains `message`."""
    with pytest.raises(MapError) as exc:
        load_docs(tmp_path, doc, **kwargs)
    assert message in str(exc.value)
