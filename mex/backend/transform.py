from enum import Enum
from typing import Any, Callable

from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel

from mex.common.types import Identifier, Timestamp

JSON_ENCODERS = {
    Enum: lambda obj: obj.value,
    Identifier: lambda obj: str(obj),
    Timestamp: lambda obj: str(obj),
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
    if isinstance(obj, BaseModel):
        # XXX sigh
        return obj.__pydantic_serializer__.to_python(
            obj,
            mode="json",
            by_alias=by_alias,
            include=include,
            exclude=exclude,
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
            exclude_none=exclude_none,
            fallback=str,
        )
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
