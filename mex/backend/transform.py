from collections.abc import Callable
from enum import Enum
from typing import Any, Final

from fastapi.encoders import jsonable_encoder

from mex.common.types import Identifier, TemporalEntity

JSON_ENCODERS: Final[dict[type, Callable[[Any], str]]] = {
    Enum: lambda obj: str(obj.value),
    Identifier: lambda obj: str(obj),
    TemporalEntity: lambda obj: str(obj),
}


def to_primitive(
    obj: Any,
    include: set[str] | None = None,
    exclude: set[str] | None = None,
    by_alias: bool = True,
    exclude_unset: bool = False,
    exclude_defaults: bool = False,
    exclude_none: bool = False,
    custom_encoder: dict[Any, Callable[[Any], Any]] = JSON_ENCODERS,
) -> Any:
    """Convert any object into python primitives compatible with JSONification."""
    return jsonable_encoder(
        obj=obj,
        include=include,
        exclude=exclude,
        by_alias=by_alias,
        exclude_unset=exclude_unset,
        exclude_defaults=exclude_defaults,
        exclude_none=exclude_none,
        custom_encoder=custom_encoder,
    )
