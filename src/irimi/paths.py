import os
from pathlib import Path

IRIMI_HOME_ENV = "IRIMI_HOME"
CA_KEY_NAME = "ca.key"
CA_CERT_NAME = "ca.pem"
REDACT_KEY_NAME = "redact.key"  # the redaction HMAC key (0600), created on first use
MITM_DIR_NAME = "mitm"
MITM_CA_BUNDLE_NAME = "mitmproxy-ca.pem"  # name mitmproxy's TlsConfig addon looks for
STORE_DIR_NAME = "store"


def irimi_home() -> Path:
    """Root directory for irimi state. $IRIMI_HOME if set, else ~/.irimi."""
    raw = os.environ.get(IRIMI_HOME_ENV)
    if raw:
        return Path(raw).expanduser()
    return Path.home() / ".irimi"


def ca_dir() -> Path:
    return irimi_home() / "ca"


def mitm_dir() -> Path:
    """mitmproxy's confdir: holds the key+cert bundle mitmproxy mints leaf certs from."""
    return irimi_home() / MITM_DIR_NAME


def store_dir() -> Path:
    """Where `irimi serve`, `irimi shadow` and `irimi replay` keep the trace store unless
    `--store` says."""
    return irimi_home() / STORE_DIR_NAME
