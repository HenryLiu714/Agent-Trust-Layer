from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src" / "irimi"
ALLOWED = {SRC / "engine" / "mitm.py"}


def test_mitmproxy_is_imported_only_in_engine_mitm():
    offenders = []
    for py in SRC.rglob("*.py"):
        if py in ALLOWED:
            continue
        for line in py.read_text().splitlines():
            stripped = line.strip()
            if stripped.startswith(("import mitmproxy", "from mitmproxy")):
                offenders.append(f"{py.relative_to(SRC)}: {stripped}")
    assert offenders == [], "mitmproxy may only be imported in irimi/engine/mitm.py:\n" + "\n".join(
        offenders
    )
