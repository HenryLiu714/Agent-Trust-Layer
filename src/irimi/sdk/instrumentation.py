"""Putting `Irimi-Run` on every request a run makes (#75). Until #75, `instrument` does nothing."""


def instrument() -> None:
    """Make the HTTP clients in this process label each request with the current run's id
    (#75). Called each time the SDK starts a run, so it must be idempotent and must not raise.
    A no-op until #75 fills it in."""
    return None
