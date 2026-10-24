"""`core.netaddr`: which hosts are this machine or loopback, and how an authority is written."""

import pytest

from irimi.core import netaddr
from irimi.core.netaddr import Target, is_loopback, is_self_host


def test_is_self_host_knows_this_machines_names():
    for host in ["localhost", "127.0.0.1", "127.255.0.1", "127.1", "::1", "::ffff:127.0.0.1"]:
        assert is_self_host(host), host
    assert is_self_host("0.0.0.0")
    assert is_self_host("::")
    for host in ["10.0.0.1", "api.stripe.com", "", "nonsense", "::ffff:10.0.0.1"]:
        assert not is_self_host(host), host


def test_is_loopback_knows_every_spelling_of_loopback():
    assert is_loopback("127.0.0.1")
    assert is_loopback("127.0.0.2")
    assert is_loopback("::1")
    assert not is_loopback("10.0.0.1")
    assert not is_loopback("")
    assert not is_loopback("nonsense")


@pytest.mark.parametrize(
    "host", ["localhost", "localhost.", "LOCALHOST", "127.0.0.1", "127.1", "0177.0.0.1", "::1"]
)
def test_the_three_loopback_predicates_agree_on_every_spelling_of_loopback(host):
    """`is_self_host`, `is_loopback` and `Target.is_loopback` agree on every loopback spelling."""
    literal = f"[{host}]" if ":" in host else host
    assert is_self_host(host) and is_loopback(host)
    assert Target.parse(f"http://{literal}:3000").is_loopback


@pytest.mark.parametrize("host", ["0.0.0.0", "::"])
def test_the_unspecified_address_is_self_but_never_a_local_target(host):
    """`0.0.0.0` is a self Host but never a loopback target.

    A request whose Host is `0.0.0.0` reaches irimi's listener; a target there reaches this machine
    only by an accident of the OS."""
    literal = f"[{host}]" if ":" in host else host
    assert is_self_host(host)
    assert not is_loopback(host)
    assert not Target.parse(f"http://{literal}:3000").is_loopback


def test_an_authority_leaves_out_the_default_port_and_brackets_ipv6():
    assert netaddr.authority("::1", 443, 443) == "[::1]"
    assert netaddr.authority("::1", 3000, 443) == "[::1]:3000"
    assert netaddr.authority("127.0.0.1", 80, 80) == "127.0.0.1"
    assert netaddr.authority("127.0.0.1", 4000) == "127.0.0.1:4000"
