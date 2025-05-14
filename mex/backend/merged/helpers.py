from typing import Any, Literal, overload

from mex.backend.graph.connector import GraphConnector
from mex.backend.merged.models import MergedItemSearch, PreviewItemSearch
from mex.backend.rules.helpers import transform_raw_rules_to_rule_set_response
from mex.common.merged.main import create_merged_item
from mex.common.models import (
    EXTRACTED_MODEL_CLASSES_BY_NAME,
    RULE_MODEL_CLASSES_BY_NAME,
    AnyMergedModel,
    AnyPreviewModel,
    ExtractedModelTypeAdapter,
)
from mex.common.types import Identifier


@overload
def merge_search_result_item(
    item: dict[str, Any],
    validate_cardinality: Literal[False],
) -> AnyPreviewModel: ...


@overload
def merge_search_result_item(
    item: dict[str, Any],
    validate_cardinality: Literal[True],
) -> AnyMergedModel: ...


def merge_search_result_item(
    item: dict[str, Any],
    validate_cardinality: Literal[True, False],
) -> AnyPreviewModel | AnyMergedModel:
    """Merge a single search result into a merged item.

    Args:
        item: Raw merged search result item from the graph response
        validate_cardinality: Merged items validate the existence of required fields and
            the lengths of lists by default, set this to `OFF` to avoid this and
            return a "preview" of a merged item instead of a valid merged item

    Raises:
        MergingError: When the given items cannot be merged

    Returns:
        Instance of a merged or preview item
    """
    extracted_items = [
        ExtractedModelTypeAdapter.validate_python(component)
        for component in item["_components"]
        if component["entityType"] in EXTRACTED_MODEL_CLASSES_BY_NAME
    ]
    raw_rules = [
        component
        for component in item["_components"]
        if component["entityType"] in RULE_MODEL_CLASSES_BY_NAME
    ]
    if raw_rules:
        rule_set = transform_raw_rules_to_rule_set_response(raw_rules)
    else:
        rule_set = None

    return create_merged_item(
        identifier=Identifier(item["identifier"]),
        extracted_items=extracted_items,
        rule_set=rule_set,
        validate_cardinality=validate_cardinality,
    )


@overload
def search_merged_items_in_graph(
    query_string: str | None = None,
    identifier: str | None = None,
    entity_type: list[str] | None = None,
    had_primary_source: list[str] | None = None,
    skip: int = 0,
    limit: int = 100,
    validate_cardinality: Literal[False] = False,  # noqa: FBT002
) -> PreviewItemSearch: ...


@overload
def search_merged_items_in_graph(
    query_string: str | None = None,
    identifier: str | None = None,
    entity_type: list[str] | None = None,
    had_primary_source: list[str] | None = None,
    skip: int = 0,
    limit: int = 100,
    validate_cardinality: Literal[True] = True,  # noqa: FBT002
) -> MergedItemSearch: ...


def search_merged_items_in_graph(  # noqa: PLR0913
    query_string: str | None = None,
    identifier: str | None = None,
    entity_type: list[str] | None = None,
    had_primary_source: list[str] | None = None,
    skip: int = 0,
    limit: int = 100,
    validate_cardinality: Literal[True, False] = True,  # noqa: FBT002
) -> PreviewItemSearch | MergedItemSearch:
    """Search for merged items.

    Args:
        query_string: Full text search query term
        identifier: Optional merged item identifier filter
        entity_type: Optional entity type filter
        had_primary_source: optional merged primary source identifier filter
        skip: How many items to skip for pagination
        limit: How many items to return at most
        validate_cardinality: Merged items validate the existence of required fields and
            the lengths of lists by default, set this to `OFF` to avoid this and
            return "previews" of merged items instead of valid merged items

    Raises:
        MergingError: When the given items cannot be merged

    Returns:
        Search response for preview or merged items
    """
    connector = GraphConnector.get()
    result = connector.fetch_merged_items(
        query_string=query_string,
        identifier=identifier,
        entity_type=entity_type,
        had_primary_source=had_primary_source,
        skip=skip,
        limit=limit,
    )
    total: int = result["total"]
    items = [
        merge_search_result_item(
            item,
            validate_cardinality=validate_cardinality,
        )
        for item in result["items"]
    ]

    if validate_cardinality is True:
        return MergedItemSearch(items=items, total=total)
    return PreviewItemSearch(items=items, total=total)
