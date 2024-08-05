from collections.abc import Callable, Mapping
from enum import Enum
from typing import Any, Final

from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from starlette.background import BackgroundTask

from mex.common.types import Identifier, TemporalEntity

JSON_ENCODERS: Final[dict[type, Callable[[Any], str]]] = {
    Enum: lambda obj: str(obj.value),
    Exception: lambda obj: str(obj),
    Identifier: lambda obj: str(obj),
    TemporalEntity: lambda obj: str(obj),
}


class MExJSONResponse(JSONResponse):
    """Custom JSON response class to correctly handle serializing MEx types."""

    def __init__(
        self,
        content: Any,
        status_code: int = 200,
        headers: Mapping[str, str] | None = None,
        media_type: str | None = None,
        background: BackgroundTask | None = None,
    ) -> None:
        """Create a new JSON response with correctly serialized content."""
        super().__init__(
            to_primitive(content), status_code, headers, media_type, background
        )


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
