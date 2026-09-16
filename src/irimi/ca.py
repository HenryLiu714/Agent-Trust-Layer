import datetime as dt
import os
from dataclasses import dataclass
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from irimi import paths

CA_COMMON_NAME = "irimi local CA"
CA_VALID_DAYS = 3650
KEY_MODE = 0o600
CERT_MODE = 0o644
DIR_MODE = 0o700


@dataclass(frozen=True)
class CAPaths:
    key: Path
    cert: Path


def ca_paths() -> CAPaths:
    d = paths.ca_dir()
    return CAPaths(key=d / paths.CA_KEY_NAME, cert=d / paths.CA_CERT_NAME)


def ca_exists(p: CAPaths) -> bool:
    """True only when both the key and the cert are present."""
    return p.key.is_file() and p.cert.is_file()


def ca_partial(p: CAPaths) -> bool:
    """True when exactly one of key/cert is present (a broken or interrupted init)."""
    return p.key.is_file() != p.cert.is_file()


def generate_ca(p: CAPaths) -> None:
    """Generate a self-signed CA and write key (0600) and cert (0644). Overwrites if present."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name(
        [
            x509.NameAttribute(NameOID.COMMON_NAME, CA_COMMON_NAME),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, "irimi"),
        ]
    )
    now = dt.datetime.now(dt.UTC)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - dt.timedelta(days=1))
        .not_valid_after(now + dt.timedelta(days=CA_VALID_DAYS))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=False,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()), critical=False)
        .sign(key, hashes.SHA256())
    )

    key_pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    cert_pem = cert.public_bytes(serialization.Encoding.PEM)

    p.key.parent.mkdir(parents=True, exist_ok=True)
    os.chmod(p.key.parent, DIR_MODE)
    _write_with_mode(p.key, key_pem, KEY_MODE)
    _write_with_mode(p.cert, cert_pem, CERT_MODE)


def _write_with_mode(path: Path, data: bytes, mode: int) -> None:
    """Create-or-truncate `path` with `mode`, never leaving it world-readable in between.

    Refuses to follow a symlink at `path`, and fixes the mode on the open descriptor
    before any bytes are written (O_CREAT's mode is subject to umask and ignored when
    the file already exists).
    """
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, mode)
    with os.fdopen(fd, "wb") as f:
        os.fchmod(fd, mode)
        f.write(data)


def write_mitm_bundle(p: CAPaths, confdir: Path) -> Path:
    """Write <confdir>/mitmproxy-ca.pem = CA key + CA cert (mode 0600), the file mitmproxy's
    TlsConfig loads to mint per-host leaf certs. Always rewritten so `init --force` is picked up."""
    bundle = confdir / paths.MITM_CA_BUNDLE_NAME
    confdir.mkdir(parents=True, exist_ok=True)
    os.chmod(confdir, DIR_MODE)
    _write_with_mode(bundle, p.key.read_bytes() + p.cert.read_bytes(), KEY_MODE)
    return bundle
