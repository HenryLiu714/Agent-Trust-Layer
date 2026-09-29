import os
from pathlib import Path

IRIMI_HOME_ENV = "IRIMI_HOME"
CA_KEY_NAME = "ca.key"
CA_CERT_NAME = "ca.pem"
REDACT_KEY_NAME = "redact.key"  # the redaction HMAC key (0600), created on first use (#69)


def irimi_home() -> Path:
    """Root directory for irimi state. $IRIMI_HOME if set, else ~/.irimi."""
    raw = os.environ.get(IRIMI_HOME_ENV)
    if raw:
        return Path(raw).expanduser()
    return Path.home() / ".irimi"


def ca_dir() -> Path:
    return irimi_home() / "ca"


MITM_DIR_NAME = "mitm"
MITM_CA_BUNDLE_NAME = "mitmproxy-ca.pem"  # name mitmproxy's TlsConfig addon looks for
LISTEN_HOST = "127.0.0.1"
DEFAULT_PORT = 4000


def mitm_dir() -> Path:
    """mitmproxy's confdir: holds the key+cert bundle mitmproxy mints leaf certs from."""
    return irimi_home() / MITM_DIR_NAME


STORE_DIR_NAME = "store"


def store_dir() -> Path:
    """Where `irimi serve` and `irimi shadow` keep the trace store unless `--store` says (#70)."""
    return irimi_home() / STORE_DIR_NAME
