from typing import Any

from pydantic import BaseModel


class TypeSerializer(BaseModel):
    """Layer for pydantic models to serialize the class name as a `$type` attribute."""

    def dict(  # type: ignore[override]
        self,
        *,
        include: set[str] | None = None,
        exclude: set[str] | None = None,
        **kwargs: Any
    ) -> dict[str, Any]:
        """Inject the class name into the dict."""
        include = include or set()
        exclude = exclude or set()
        include_type = "$type" in include or "$type" not in exclude
        include = include or None
        exclude = exclude or None
        dct = super().dict(include=include, exclude=exclude, **kwargs)
        if include_type:
            dct["$type"] = self.__class__.__name__
        return dct
