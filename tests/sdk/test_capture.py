"""`sdk._capture`: one test per row of its table of argument captures, and `revive` for replay."""

import dataclasses
import enum
import sys
import time
import types
from typing import Any

import pytest

from irimi import sdk
from irimi.core import limits
from irimi.sdk import _capture as capture

HERE = __name__  # the module a trigger defined in this file records in its entrypoint


# -- capture: one test per row of the table (sdk/_capture.py, docs/trace-format.md) --------------


@dataclasses.dataclass
class Ticket:
    ticket_id: str
    amount: int
    note: str = dataclasses.field(default="", init=False)


class Model:
    """Shaped like a pydantic v2 model: a callable `model_dump(mode="json")`."""

    def __init__(self, **fields: Any) -> None:
        self.fields = fields

    def model_dump(self, mode: str = "python") -> dict[str, Any]:
        assert mode == "json"
        return dict(self.fields)


class Color(enum.IntEnum):
    RED = 1


class Unprintable:
    def __repr__(self) -> str:
        raise RuntimeError("no")


@pytest.mark.parametrize(
    ("value", "encoded"),
    [(None, None), (True, True), (0, 0), (-12, -12), ("é", "é"), (1.5, 1.5), (0.0, 0.0)],
)
def test_a_json_scalar_is_itself_and_replayable(value, encoded):
    assert capture.capture(value) == (encoded, True)


@pytest.mark.parametrize(("value", "text"), [(float("nan"), "nan"), (float("inf"), "inf"),
                                             (float("-inf"), "-inf")])  # fmt: skip
def test_a_non_finite_float_is_its_repr_and_not_replayable(value, text):
    assert capture.capture(value) == ({"__irimi_repr__": text}, False)


def test_a_list_and_a_tuple_are_json_lists_replayable_when_every_item_is():
    assert capture.capture([1, (2, "x")]) == ([1, [2, "x"]], True)
    assert capture.capture((1, float("nan"))) == ([1, {"__irimi_repr__": "nan"}], False)


def test_a_dict_with_string_keys_is_an_object_replayable_when_every_value_is():
    assert capture.capture({"a": 1, "b": [None]}) == ({"a": 1, "b": [None]}, True)
    assert capture.capture({"a": b"\x00"}) == ({"a": {"__irimi_bytes__": "AA=="}}, True)
    assert capture.capture({"a": object})[1] is False


def test_a_dict_with_any_other_key_is_its_repr_and_not_replayable():
    assert capture.capture({1: "a"}) == ({"__irimi_repr__": "{1: 'a'}"}, False)
    assert capture.capture({"a": 1, ("t",): 2})[1] is False


def test_a_dict_holding_a_key_capture_writes_is_its_repr_so_a_marker_is_never_ambiguous():
    value = {"__irimi_bytes__": "AA=="}
    assert capture.capture(value) == ({"__irimi_repr__": repr(value)}, False)


def test_a_dataclass_is_its_importable_class_and_its_init_fields():
    assert capture.capture(Ticket("T-1", 500)) == (
        {"__irimi_type__": f"{HERE}:Ticket", "value": {"ticket_id": "T-1", "amount": 500}},
        True,
    )


def test_a_dataclass_is_replayable_only_when_its_fields_are_and_its_class_can_be_imported():
    @dataclasses.dataclass
    class Local:
        x: int

    encoded, replayable = capture.capture(Local(1))
    assert encoded == {"__irimi_type__": f"{HERE}:{Local.__qualname__}", "value": {"x": 1}}
    assert "<locals>" in Local.__qualname__ and replayable is False
    assert capture.capture(Ticket("T-1", float("nan")))[1] is False  # type: ignore[arg-type]


def test_an_object_with_model_dump_is_its_class_and_its_json_dump():
    assert capture.capture(Model(id="cus_1", tags=["a"])) == (
        {"__irimi_type__": f"{HERE}:Model", "value": {"id": "cus_1", "tags": ["a"]}},
        True,
    )


def test_bytes_are_base64_and_replayable():
    assert capture.capture(b"hi\xff") == ({"__irimi_bytes__": "aGn/"}, True)


@pytest.mark.parametrize(
    "value",
    [object(), Color.RED, bytearray(b"x"), {1, 2}, lambda: 1],
    ids=["object", "IntEnum", "bytearray", "set", "function"],
)
def test_anything_else_is_its_repr_and_not_replayable(value):
    assert capture.capture(value) == ({"__irimi_repr__": repr(value)[:1000]}, False)


def test_a_repr_is_cut_to_1000_characters_outside_secrets():
    """A repr is cut before redaction, so never inside a key: a key across the cut is left out.

    A repr with no key across the cut keeps 1000 characters, one long word included."""

    class Long:
        def __init__(self, text: str) -> None:
            self.text = text

        def __repr__(self) -> str:
            return self.text

    assert capture.capture(Long("x " * 2500)) == ({"__irimi_repr__": "x " * 500}, False)
    keyed = Long("x" * 990 + " ghp_" + "k" * 36)
    assert capture.capture(keyed) == ({"__irimi_repr__": "x" * 990 + " "}, False)
    assert capture.capture(Long("x" * 5000)) == ({"__irimi_repr__": "x" * 1000}, False)
    openai = Long("x" * 985 + "'sk-" + "k" * 40 + "'")
    assert capture.capture(openai) == ({"__irimi_repr__": "x" * 985 + "'"}, False)


def test_a_repr_that_raises_is_named_unrepresentable():
    assert capture.capture(Unprintable()) == (
        {"__irimi_repr__": "<unrepresentable Unprintable>"},
        False,
    )


def test_a_model_dump_that_raises_is_its_repr_and_never_the_agents_error():
    class Broken:
        def model_dump(self, mode: str) -> dict[str, Any]:
            raise ValueError("no")

        def __repr__(self) -> str:
            return "<Broken>"

    assert capture.capture(Broken()) == ({"__irimi_repr__": "<Broken>"}, False)


def test_an_int_too_long_to_write_is_unrepresentable():
    assert capture.capture(10**5000) == ({"__irimi_repr__": "<unrepresentable int>"}, False)


def test_nesting_deeper_than_32_levels_becomes_too_deep():
    value: list[Any] = []
    innermost = value
    for _ in range(39):  # 40 lists, one inside the next
        innermost.append([])
        innermost = innermost[0]
    encoded, replayable = capture.capture(value)
    depth = 0
    while isinstance(encoded, list):
        [encoded] = encoded
        depth += 1
    assert (depth, encoded, replayable) == (33, {"__irimi_repr__": "<too deep>"}, False)
    ok: list[Any] = [1]
    for _ in range(31):
        ok = [ok]
    assert capture.capture(ok)[1] is True  # 32 lists deep, every item reached


def test_args_past_1_mib_are_replaced_by_their_size():
    big = "x" * (2 * 1024 * 1024)
    assert capture.capture(big) == ({"__irimi_truncated__": True, "bytes": len(big) + 2}, False)
    under = "x" * (1024 * 1024 - 2)
    assert capture.capture(under) == (under, True)


def test_args_that_grow_past_1_mib_when_written_are_replaced_by_their_exact_size():
    escaped = "\x00" * (300 * 1024)  # 300 KiB that JSON writes as six bytes each
    encoded, replayable = capture.capture(escaped)
    assert encoded == {"__irimi_truncated__": True, "bytes": len(capture.serialized(escaped))}
    assert replayable is False


def test_a_value_that_shares_itself_is_bounded_in_time():
    shared: list[Any] = []
    shared.extend([shared, shared])
    began = time.monotonic()
    encoded, replayable = capture.capture(shared)
    assert time.monotonic() - began < 10
    assert isinstance(encoded, dict) and encoded["__irimi_truncated__"] is True
    size = encoded["bytes"]
    assert isinstance(size, int)
    assert size > limits.MAX_CAPTURED_BYTES and replayable is False


def test_capture_call_binds_arguments_by_parameter_name():
    def f(a: int, b: int = 2, *rest: int, c: str = "", **more: Any) -> None: ...

    assert capture.capture_call(f, (1, 3, 4), {"c": "x", "d": True}) == (
        {"a": 1, "b": 3, "rest": [4], "c": "x", "more": {"d": True}},
        True,
    )
    assert capture.capture_call(f, (1,), {}) == ({"a": 1}, True)  # defaults are not applied


def test_capture_call_leaves_out_a_receiver_named_self_or_cls():
    class Thing:
        def method(self, x: int) -> None: ...

        @classmethod
        def build(cls, x: int) -> None: ...

    assert capture.capture_call(Thing.method, (Thing(), 1), {}) == ({"x": 1}, True)
    assert capture.capture_call(Thing.build.__func__, (Thing, 1), {}) == ({"x": 1}, True)  # type: ignore[attr-defined]

    def plain(other: Any, x: int) -> None: ...

    assert capture.capture_call(plain, (object(), 1), {})[1] is False


def _self_first(self: int, x: int) -> None: ...


class _Statics:
    @staticmethod
    def outside(self: int, x: int) -> None: ...

    @sdk.trigger
    @staticmethod
    def trigger_outside(cls: int, x: int) -> None: ...

    @staticmethod
    @sdk.trigger
    def trigger_inside(self: int, x: int) -> None: ...

    @classmethod
    def build(cls, self: int, x: int) -> None: ...


def test_capture_call_keeps_a_first_parameter_named_self_or_cls_that_receives_nothing():
    """A plain function's or static method's first parameter is an argument, whatever its name.

    Leaving it out would make the run one replay cannot call again."""

    def inner(cls: int, x: int) -> None: ...

    assert capture.capture_call(_self_first, (1, 2), {}) == ({"self": 1, "x": 2}, True)
    assert capture.capture_call(inner, (1, 2), {}) == ({"cls": 1, "x": 2}, True)
    assert capture.capture_call(_Statics.outside, (1, 2), {}) == ({"self": 1, "x": 2}, True)
    # What `@sdk.trigger` captures: the function it wraps, in either decorator order.
    outside = _Statics.trigger_outside.__wrapped__  # type: ignore[attr-defined]
    inside = _Statics.trigger_inside.__wrapped__  # type: ignore[attr-defined]
    assert capture.capture_call(outside, (1, 2), {}) == ({"cls": 1, "x": 2}, True)
    assert capture.capture_call(inside, (1, 2), {}) == ({"self": 1, "x": 2}, True)
    # A bound method's receiver is bound already: its first parameter left is an argument.
    bound = _Statics.build
    assert capture.capture_call(bound, (1, 2), {}) == ({"self": 1, "x": 2}, True)


def test_a_call_that_does_not_bind_is_captured_as_args_and_kwargs_and_not_replayable():
    def f(a: int) -> None: ...

    assert capture.capture_call(f, (1, 2), {"z": 3}) == (
        {"args": [1, 2], "kwargs": {"z": 3}},
        False,
    )


def test_a_dataclass_defined_in_main_is_not_replayable():
    """Replay imports the class by name in another process, where `__main__` is another module."""
    ticket = dataclasses.make_dataclass("Ticket", ["x"], module="__main__")
    assert capture.capture(ticket(1)) == (
        {"__irimi_type__": "__main__:Ticket", "value": {"x": 1}},
        False,
    )


def test_a_trigger_parameter_may_nest_as_deep_as_a_run_trigger():
    deep: list[Any] = [1]
    for _ in range(31):
        deep = [deep]  # 32 lists deep

    def f(value: Any) -> None: ...

    assert capture.capture(deep)[1] is True
    assert capture.capture_call(f, (deep,), {})[1] is True
    assert capture.capture_call(f, ([deep],), {})[1] is False


# -- revive: capture's inverse, one test per row -------------------------------------------------


class Validated:  # noqa: PLW1641  # unhashable, as a pydantic model is
    """Shaped like a pydantic v2 model both ways: `model_dump(mode="json")` and `model_validate`."""

    def __init__(self, **fields: Any) -> None:
        self.fields = fields

    def model_dump(self, mode: str = "python") -> dict[str, Any]:
        return dict(self.fields)

    @classmethod
    def model_validate(cls, value: Any) -> "Validated":
        return cls(**value)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Validated) and other.fields == self.fields


@pytest.mark.parametrize(
    "value", [None, True, 0, -12, "é", 1.5, [1, [2, "x"]], {"a": {"b": [None]}}]
)
def test_revive_gives_json_back_as_itself(value):
    assert capture.revive(value) == value


def test_revive_gives_a_captured_tuple_back_as_a_list():
    assert capture.revive(capture.capture((1, (2, "x")))[0]) == [1, [2, "x"]]


def test_revive_gives_bytes_back():
    assert capture.revive({"__irimi_bytes__": "AA=="}) == b"\x00"
    assert capture.revive(capture.capture(b"\xffraw")[0]) == b"\xffraw"


def test_revive_rebuilds_a_dataclass_from_its_init_fields():
    revived = capture.revive(capture.capture(Ticket("T1", 4900))[0])
    assert type(revived) is Ticket and revived == Ticket("T1", 4900)


def test_revive_rebuilds_a_model_with_model_validate():
    assert capture.revive(capture.capture(Validated(a=1, b=[2]))[0]) == Validated(a=1, b=[2])


@pytest.mark.parametrize(
    "value",
    [
        {"__irimi_repr__": "Decimal('0.87')"},
        {"__irimi_truncated__": True, "bytes": 2_000_000},
        [1, {"x": {"__irimi_repr__": "nan"}}],
    ],
)
def test_revive_refuses_a_description_anywhere_in_the_value(value):
    with pytest.raises(capture.NotReplayable, match="its description"):
        capture.revive(value)


@pytest.mark.parametrize(
    ("value", "said"),
    [
        ({"__irimi_type__": "tests.sdk.test_capture:Nowhere", "value": {}}, "not in its module"),
        ({"__irimi_type__": "no.such.module:Ticket", "value": {}}, "has not imported"),
        ({"__irimi_type__": "tests.sdk.test_capture:HERE", "value": {}}, "is not a class"),
        (
            {"__irimi_type__": "tests.sdk.test_capture:Model", "value": {}},
            "neither a dataclass nor a model",
        ),
        (
            {"__irimi_type__": "tests.sdk.test_capture:Ticket", "value": {"nope": 1}},
            "could not be built",
        ),
        (
            {"__irimi_type__": "tests.sdk.test_capture:Ticket", "value": [1]},
            "fields are not an object",
        ),
        ({"__irimi_type__": 7, "value": {}}, "names no class"),
        ({"__irimi_bytes__": "not base64!"}, "not base64"),
        ({"__irimi_bytes__": "é"}, "not base64"),
        ({"__irimi_bytes__": 12}, "not base64"),
    ],
)
def test_revive_names_why_a_value_cannot_be_rebuilt(value, said):
    with pytest.raises(capture.NotReplayable, match=said):
        capture.revive(value)


def test_revive_never_imports_a_module_a_stored_run_names(monkeypatch):
    """revive finds a class only in a module already imported: a stored run is data, not code."""
    monkeypatch.delitem(sys.modules, "json.tool", raising=False)
    with pytest.raises(capture.NotReplayable, match="has not imported"):
        capture.revive({"__irimi_type__": "json.tool:main", "value": {}})
    assert "json.tool" not in sys.modules


def test_revive_never_asks_a_module_s_getattr(monkeypatch):
    """revive looks only in a module's own namespace, so a lazy `__getattr__` cannot import."""
    asked: list[str] = []
    lazy = types.ModuleType("lazy_pkg")

    def lazy_getattr(name: str) -> Any:
        asked.append(name)
        return Ticket

    lazy.__getattr__ = lazy_getattr  # type: ignore[method-assign]
    monkeypatch.setitem(sys.modules, "lazy_pkg", lazy)
    with pytest.raises(capture.NotReplayable, match="not in its module"):
        capture.revive({"__irimi_type__": "lazy_pkg:Ticket", "value": {"ticket_id": "T1"}})
    assert asked == []


def test_revive_raises_nothing_but_not_replayable_however_deep_the_value():
    deep: Any = []
    for _ in range(100_000):
        deep = [deep]
    with pytest.raises(capture.NotReplayable):
        capture.revive(deep)
