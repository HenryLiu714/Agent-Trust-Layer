"""Fixtures every test module can ask for, registered through `pytest_plugins`."""

import logging

import pytest

from irimi.config import ca, paths
from irimi.decide.policy import ShadowPolicy
from irimi.sdk import _control_client as client
from tests.support.engine import engine_config, running_irimi, start_engine
from tests.support.maps import PRECONDITION_STRIPE_MAP, complete_for_check, map_index
from tests.support.scenarios import base_url_stripe
from tests.support.upstreams import (
    FakeLlm,
    FakeStripe,
    ReplayStubs,
    TargetStub,
    Upstream,
    precondition_stripe,
    refusing_port,
    replay_llm,
    replay_stripe,
    serve,
)


@pytest.fixture
def upstream():
    """A plain `Upstream` on loopback; `upstream.seen` is what it was asked."""
    port = serve(Upstream)
    yield port
    port.stop()


@pytest.fixture
def engine(tmp_path, monkeypatch):
    """A running engine over no maps: `(engine, exchanges it reported)`."""
    eng, seen, stop = start_engine(engine_config(tmp_path, monkeypatch))
    yield eng, seen
    stop()


@pytest.fixture
def target():
    """A `TargetStub` an answer target can name; `target.stop()` frees its port mid-test, so the
    next connection to it is refused."""
    port = serve(TargetStub)
    yield port
    port.stop()


@pytest.fixture
def precondition_stub(tmp_path, monkeypatch):
    """The stub, and an engine over PRECONDITION_STRIPE_MAP built the way the CLI builds one: the
    real overlay and a policy holding the real reader. Yields (proxy port, stub port, exchanges)."""
    from irimi.decide.overlay import ServiceOverlay
    from irimi.decide.reader import UpstreamReader

    stripe = serve(FakeStripe, threaded=True, **precondition_stripe())
    maps = map_index(tmp_path, monkeypatch, doc=PRECONDITION_STRIPE_MAP)
    eng, seen, stop = start_engine(
        engine_config(tmp_path, monkeypatch, maps=maps),
        overlay=ServiceOverlay(),
        policy=ShadowPolicy(reader=UpstreamReader(), maps=maps),
    )
    yield eng.listen_port(), stripe, seen
    stop()
    stripe.stop()


@pytest.fixture
def stripe_stand_in():
    """The base-URL refund's Stripe. Threaded: irimi's own precondition read runs on a worker
    thread, and a single-threaded stand-in would serialize it against the agent's traffic."""
    port = serve(FakeStripe, threaded=True, **base_url_stripe())
    yield port
    port.stop()


@pytest.fixture
def home(tmp_path, monkeypatch):
    """`$IRIMI_HOME` in this test's temporary directory, with a CA generated."""
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path))
    ca.generate_ca(ca.ca_paths())
    return tmp_path


@pytest.fixture
def home_with_maps(tmp_path, monkeypatch):
    """`home_with_maps({"stripe.yaml": text, ...})`: those maps in place of the shipped ones,
    completed with what the load-time check requires. Returns the maps directory."""

    def install(maps: dict[str, str]):
        maps_dir = tmp_path / "maps"
        maps_dir.mkdir()
        for name, text in maps.items():
            (maps_dir / name).write_text(text)
        complete_for_check(maps_dir)
        monkeypatch.setattr("irimi.servicemaps.loader.shipped_dir", lambda: maps_dir)
        return maps_dir

    return install


@pytest.fixture
def irimi(tmp_path, monkeypatch):
    """A running engine over no maps, with a real store and the SDK set up to report to it."""
    with running_irimi(engine_config(tmp_path, monkeypatch), tmp_path, monkeypatch) as running:
        yield running


@pytest.fixture
def warnings(caplog):
    """`warnings()`: what the SDK has logged so far at WARNING or above on `irimi.sdk`."""
    caplog.set_level(logging.WARNING, logger=client.LOGGER_NAME)
    return lambda: [r for r in caplog.records if r.name == client.LOGGER_NAME]


@pytest.fixture
def refused_port():
    """A free loopback port nothing listens on, so a connection to it is refused."""
    return refusing_port()


@pytest.fixture
def replay_stubs():
    """A Stripe and a model for a run to record and a replay to answer from: `(stripe, llm)`."""
    stripe = serve(FakeStripe, threaded=True, **replay_stripe())
    llm = serve(FakeLlm, threaded=True, **replay_llm())
    yield ReplayStubs(stripe, llm)
    stripe.stop()
    llm.stop()
