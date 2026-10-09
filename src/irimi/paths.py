import os
from pathlib import Path

IRIMI_HOME_ENV = "IRIMI_HOME"
CA_KEY_NAME = "ca.key"
CA_CERT_NAME = "ca.pem"
REDACT_KEY_NAME = "redact.key"  # the redaction HMAC key (0600), created on first use (#69)

# What `irimi shadow` tells its child (`runner.child_env`), declared here, in layer 0, so the SDK
# can import the names it reads (#73). `IRIMI_ENGINE_ACTIVE=1` says a proxy is in front of the
# process, `IRIMI_RUN` names the process run (#70), and `IRIMI_CONTROL` is the base URL of the
# control endpoint, `http://<host>:<port>/_irimi`, where the SDK reports what the wire cannot show.
ENGINE_ACTIVE_ENV = "IRIMI_ENGINE_ACTIVE"
RUN_ENV = "IRIMI_RUN"
CONTROL_ENV = "IRIMI_CONTROL"
# The proxy variables `irimi shadow` points at its own listener, in both spellings, since clients
# disagree on which they read. Declared once, here, because the SDK reads these same names to tell
# a connection to irimi from one that bypasses it (#75).
PROXY_ENVS = ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy")
# The agent's own version, if its deployment names one. `irimi shadow` records it on the process
# run (#70) and the SDK on each run it starts (#74), so both read this one name.
AGENT_VERSION_ENV = "IRIMI_AGENT_VERSION"


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
