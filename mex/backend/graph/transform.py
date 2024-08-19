from typing import Any, TypedDict, cast

from pydantic import BaseModel


class _SearchResultReference(TypedDict):
    """Helper class to show the structure of search result references."""

    label: str  # corresponds to the field name of the pydantic model
    position: int  # if the field in pydantic is a list this helps keep its order
    value: str | dict[str, str | None]  # this can be a raw Identifier, Text or Link


def expand_references_in_search_result(item: dict[str, Any]) -> None:
    """Expand the `_refs` collection in a search result item.

    Each item in a search result has a collection of `_refs` in the form of
    `_SearchResultReference`. Before parsing them into pydantic, we need to inline
    the references back into the `item` dictionary.
    """
    # XXX if we can use `apoc`, we might do this step directly in the cypher query
    for ref in cast(list[_SearchResultReference], item.pop("_refs")):
        target_list = item.setdefault(ref["label"], [None])
        length_needed = 1 + ref["position"] - len(target_list)
        target_list.extend([None] * length_needed)
        target_list[ref["position"]] = ref["value"]


def to_primitive(
    obj: BaseModel,
    include: set[str] | None = None,
    exclude: set[str] | None = None,
    by_alias: bool = True,
    exclude_unset: bool = False,
    exclude_defaults: bool = False,
    exclude_none: bool = False,
) -> Any:
    """Convert model object into python primitives compatible with graph ingestion."""
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
