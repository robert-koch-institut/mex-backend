from types import NoneType
from typing import Any, TypeGuard, TypeVar, cast

from mex.common.models import BaseModel

KEY_SEPARATOR = "_"
NULL_VALUE = ""
DehydratedValue = str | list[str]
HydratedValue = str | None | list[str | None]
NestedValues = str | None | list["NestedValues"] | dict[str, "NestedValues"]
NestedDict = dict[str, NestedValues]
FlatDict = dict[str, str | list[str]]
KeyPath = tuple[str | int, ...]
AnyT = TypeVar("AnyT", bound=Any)
ModelT = TypeVar("ModelT", bound=BaseModel)


def are_instances(value: Any, *types: type[AnyT]) -> TypeGuard[list[AnyT]]:
    """Return whether value is a list of types or subclasses thereof."""
    return isinstance(value, list) and all(isinstance(v, types) for v in value)


def dehydrate_value(value: HydratedValue) -> DehydratedValue:
    """Convert any pydantic value into something we can store in the graph."""
    if value is None:
        return NULL_VALUE
    if isinstance(value, str):
        return value
    if are_instances(value, str, NoneType):
        return cast(list[str], [dehydrate_value(cast(str | None, v)) for v in value])
    raise TypeError("can only dehydrate strings or lists of strings")


def hydrate_value(value: DehydratedValue) -> HydratedValue:
    """Convert a value stored in the graph into something we can parse with pydantic."""
    if value == NULL_VALUE:
        return None
    if isinstance(value, str):
        return value
    if are_instances(value, str):
        return cast(list[str | None], [hydrate_value(v) for v in value])
    raise TypeError("can only hydrate strings or lists of strings")


def dehydrate(nested: NestedDict) -> FlatDict:  # noqa: C901
    """Convert a nested pydantic dict into a flattened and dehydrated graph node.

    Args:
        nested: Dictionary with nested dictionary and at most one list per key-path

    Returns:
        Dictionary with flat key-value-pairs
    """

    def _dehydrate(  # noqa: C901
        in_: list[NestedValues] | dict[str, NestedValues],
        out: FlatDict,
        parent: KeyPath,
    ) -> bool:
        """Dehydrate the `in_` structure into an `out` dictionary.

        Args:
            in_: List or dictionary of allowed values
            out: Dictionary to write flat key-value-pairs into
            parent: The parent key path for recursions

        Returns:
            Whether this path needs to be recursed further
        """
        key_value_iterable = enumerate(in_) if isinstance(in_, list) else in_.items()
        has_item = False
        for key, value in key_value_iterable:
            has_item = True
            key_path = parent + cast(KeyPath, (key,))
            if isinstance(value, (dict, list)):
                has_child = _dehydrate(value, out, key_path)
                if has_child or not isinstance(value, (dict, list)):
                    continue
            position = None
            flat_key = ""
            for key_in_path in key_path:
                if isinstance(key_in_path, int):
                    if position is not None:
                        raise TypeError("can only handle one list per path")
                    position = key_in_path
                elif flat_key:
                    flat_key = KEY_SEPARATOR.join((flat_key, key_in_path))
                else:
                    flat_key = key_in_path
            if position is not None:
                out.setdefault(flat_key, [])
                if not isinstance(out[flat_key], list):  # pragma: no cover
                    raise RuntimeError("this key path should have been a list")
                length_needed = 1 + position - len(out[flat_key])
                out[flat_key].extend([NULL_VALUE] * length_needed)  # type: ignore
                out[flat_key][position] = dehydrate_value(value)  # type: ignore
            elif flat_key in out:  # pragma: no cover
                raise RuntimeError("already dehydrated this key path")
            else:
                out[flat_key] = dehydrate_value(value)  # type: ignore
        return has_item

    flat: FlatDict = {}
    _dehydrate(nested, flat, ())
    return flat


def hydrate(flat: FlatDict, model: type[BaseModel]) -> NestedDict:  # noqa: C901
    """Convert a flattened and dehydrated graph node into a nested dict for pydantic.

    Args:
        flat: Dictionary with flat key-value-pairs
        model: MEx model class to infer structure from

    Returns:
        A nested dictionary conforming to the model structure
    """
    nested: NestedDict = {}
    for flat_key, value in flat.items():
        key_path = flat_key.split(KEY_SEPARATOR)
        key_path_length = len(key_path)
        value_count = len(value)
        value_is_list = isinstance(value, list)
        model_at_depth = model
        target = nested
        for depth, key in enumerate(key_path, start=1):
            if depth < key_path_length:
                if not issubclass(model_at_depth, BaseModel):
                    raise TypeError("cannot hydrate paths with non base models")
                if key in model_at_depth._get_list_field_names():
                    if not value_is_list:
                        raise TypeError("cannot hydrate non-list to list")
                    if isinstance(target, list):
                        raise TypeError("cannot handle multiple list branches")
                    target = target.setdefault(key, [{} for _ in range(value_count)])  # type: ignore
                elif isinstance(target, list):
                    if len(target) != value_count:  # pragma: no cover
                        raise RuntimeError("branch count must match our values")
                    target = [t.setdefault(key, {}) for t in target]
                else:
                    target = target.setdefault(key, {})  # type: ignore
            elif isinstance(target, list):
                for t, v in zip(target, value):
                    t[key] = hydrate_value(v)
            else:
                target[key] = hydrate_value(value)  # type: ignore
            try:
                model_at_depth = model_at_depth.__fields__[key].type_
            except KeyError:
                raise TypeError("flat dict does not align with target model")

    return nested
