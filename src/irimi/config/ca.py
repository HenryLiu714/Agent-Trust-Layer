import datetime as dt
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from irimi.config import paths
from irimi.config.files import DIR_MODE, FILE_MODE, private_dir, write_private

CA_COMMON_NAME = "irimi local CA"
# About two years, so a forgotten trusted CA stops working; `irimi init --force` makes a new one.
CA_VALID_DAYS = 825
# How long before the CA expires the commands that start a proxy warn that it will.
EXPIRY_WARNING = dt.timedelta(days=30)
KEY_MODE = FILE_MODE
CERT_MODE = 0o644


@dataclass(frozen=True)
class CAPaths:
    key: Path
    cert: Path


def ca_paths() -> CAPaths:
    d = paths.ca_dir()
    return CAPaths(key=d / paths.CA_KEY_NAME, cert=d / paths.CA_CERT_NAME)


def ca_exists(p: CAPaths) -> bool:
    """Return True only when both the key and the cert are present."""
    return p.key.is_file() and p.cert.is_file()


def ca_partial(p: CAPaths) -> bool:
    """Return True when exactly one of key/cert is present (a broken or interrupted init)."""
    return p.key.is_file() != p.cert.is_file()


def state(p: CAPaths) -> Literal["absent", "partial", "present"]:
    """Return whether both CA files, exactly one (an interrupted init), or neither are present."""
    if ca_exists(p):
        return "present"
    return "partial" if ca_partial(p) else "absent"


def expires_at(cert: Path) -> dt.datetime | None:
    """Return when the CA certificate at `cert` expires, in UTC, or None if it cannot be read."""
    try:
        return x509.load_pem_x509_certificate(cert.read_bytes()).not_valid_after_utc
    except (OSError, ValueError):
        return None


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

    private_dir(p.key.parent)
    os.chmod(p.key.parent, DIR_MODE)  # an existing directory too
    write_private(p.key, key_pem, mode=KEY_MODE)
    write_private(p.cert, cert_pem, mode=CERT_MODE)


def write_mitm_bundle(p: CAPaths, confdir: Path) -> Path:
    """Write `<confdir>/mitmproxy-ca.pem`, the CA key plus cert (mode 0600), and return its path.

    mitmproxy's TlsConfig loads it to mint per-host leaf certs. It is always rewritten, so
    `init --force` is picked up."""
    bundle = confdir / paths.MITM_CA_BUNDLE_NAME
    private_dir(confdir)
    os.chmod(confdir, DIR_MODE)  # an existing directory too
    write_private(bundle, p.key.read_bytes() + p.cert.read_bytes(), mode=KEY_MODE)
    return bundle
