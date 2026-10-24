"""Amounts in minor units, as a person reads them: `format_minor_units(4900, "usd")` -> `$49.00`.

Only fields a map's `human:` template marks (`{amount:money}`) are formatted as money.
"""

# Stripe's zero-decimal list: ¥4900 is four thousand nine hundred yen, not ¥49.00.
ZERO_DECIMAL_CURRENCIES: frozenset[str] = frozenset(
    {
        "bif",
        "clp",
        "djf",
        "gnf",
        "jpy",
        "kmf",
        "krw",
        "mga",
        "pyg",
        "rwf",
        "ugx",
        "vnd",
        "vuv",
        "xaf",
        "xof",
        "xpf",
    }
)
CURRENCY_SYMBOLS: dict[str, str] = {"usd": "$", "eur": "€", "gbp": "£", "jpy": "¥"}


def format_minor_units(minor_units: int, currency: str) -> str:
    """Return `minor_units` of `currency` (any case) with its symbol, or its code if it has none."""
    code = currency.lower()
    figure = f"{minor_units}" if code in ZERO_DECIMAL_CURRENCIES else f"{minor_units / 100:.2f}"
    symbol = CURRENCY_SYMBOLS.get(code)
    return f"{symbol}{figure}" if symbol else f"{figure} {code.upper()}"
