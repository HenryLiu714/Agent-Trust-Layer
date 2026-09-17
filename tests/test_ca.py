import stat

from cryptography import x509
from cryptography.hazmat.primitives import serialization

from irimi import ca, paths


def _paths(tmp_path, monkeypatch):
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path))
    return ca.ca_paths()


def test_generate_writes_key_and_cert_with_modes(tmp_path, monkeypatch):
    p = _paths(tmp_path, monkeypatch)
    ca.generate_ca(p)
    assert stat.S_IMODE(p.key.stat().st_mode) == 0o600
    assert stat.S_IMODE(p.cert.stat().st_mode) == 0o644
    assert stat.S_IMODE(p.key.parent.stat().st_mode) == 0o700


def test_generated_cert_is_a_ca(tmp_path, monkeypatch):
    p = _paths(tmp_path, monkeypatch)
    ca.generate_ca(p)
    cert = x509.load_pem_x509_certificate(p.cert.read_bytes())
    key = serialization.load_pem_private_key(p.key.read_bytes(), password=None)
    bc = cert.extensions.get_extension_for_class(x509.BasicConstraints)
    assert bc.critical and bc.value.ca is True
    assert cert.subject == cert.issuer
    assert cert.public_key().public_numbers() == key.public_key().public_numbers()


def test_exists_and_partial(tmp_path, monkeypatch):
    p = _paths(tmp_path, monkeypatch)
    assert not ca.ca_exists(p) and not ca.ca_partial(p)
    ca.generate_ca(p)
    assert ca.ca_exists(p) and not ca.ca_partial(p)
    p.cert.unlink()
    assert not ca.ca_exists(p) and ca.ca_partial(p)


def test_generate_overwrites_and_fixes_mode(tmp_path, monkeypatch):
    p = _paths(tmp_path, monkeypatch)
    ca.generate_ca(p)
    p.key.chmod(0o644)
    before = p.key.read_bytes()
    ca.generate_ca(p)
    assert p.key.read_bytes() != before
    assert stat.S_IMODE(p.key.stat().st_mode) == 0o600


def test_write_mitm_bundle_is_key_then_cert_with_modes(tmp_path, monkeypatch):
    p = _paths(tmp_path, monkeypatch)
    ca.generate_ca(p)
    confdir = tmp_path / "mitm"
    bundle = ca.write_mitm_bundle(p, confdir)
    assert bundle == confdir / "mitmproxy-ca.pem"
    assert stat.S_IMODE(bundle.stat().st_mode) == 0o600
    assert stat.S_IMODE(confdir.stat().st_mode) == 0o700
    assert bundle.read_bytes() == p.key.read_bytes() + p.cert.read_bytes()


def test_write_mitm_bundle_picks_up_regenerated_ca(tmp_path, monkeypatch):
    p = _paths(tmp_path, monkeypatch)
    confdir = tmp_path / "mitm"
    ca.generate_ca(p)
    old = ca.write_mitm_bundle(p, confdir).read_bytes()
    ca.generate_ca(p)
    bundle = ca.write_mitm_bundle(p, confdir)
    assert bundle.read_bytes() != old
    assert bundle.read_bytes() == p.key.read_bytes() + p.cert.read_bytes()
