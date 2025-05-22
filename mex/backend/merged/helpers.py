from typing import Any, Literal, cast, overload

from pydantic import ValidationError

from mex.backend.graph.connector import GraphConnector
from mex.backend.rules.helpers import transform_raw_rules_to_rule_set_response
from mex.backend.types import Validation
from mex.common.exceptions import MergingError
from mex.common.merged.main import create_merged_item
from mex.common.models import (
    EXTRACTED_MODEL_CLASSES_BY_NAME,
    RULE_MODEL_CLASSES_BY_NAME,
    AnyMergedModel,
    AnyPreviewModel,
    ExtractedModelTypeAdapter,
    PaginatedItemsContainer,
)
from mex.common.types import Identifier


@overload
def merge_search_result_item(
    item: dict[str, Any],
    validation: Literal[Validation.LENIENT],
) -> AnyPreviewModel: ...


@overload
def merge_search_result_item(
    item: dict[str, Any],
    validation: Literal[Validation.STRICT],
) -> AnyMergedModel: ...


@overload
def merge_search_result_item(
    item: dict[str, Any],
    validation: Literal[Validation.IGNORE],
) -> AnyMergedModel | None: ...


def merge_search_result_item(
    item: dict[str, Any],
    validation: Literal[Validation.STRICT, Validation.LENIENT, Validation.IGNORE],
) -> AnyPreviewModel | AnyMergedModel | None:
    """Merge a single search result into a merged item.

    Args:
        item: Raw merged search result item from the graph response
        validation: Merged items validate the existence of required fields and
            the lengths of lists by default, set this to `LENIENT` to avoid this and
            return a "preview" of a merged item instead of a valid merged item,
            or set this to `IGNORE` to return None in case of validation errors.

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
    try:
        return create_merged_item(
            identifier=Identifier(item["identifier"]),
            extracted_items=extracted_items,
            rule_set=rule_set,
            validate_cardinality=cast(
                "Literal[True, False]", validation != Validation.LENIENT
            ),
        )
    except (MergingError, ValidationError):
        if validation == Validation.STRICT:
            raise
    return None


@overload
def search_merged_items_in_graph(
    query_string: str | None = None,
    identifier: str | None = None,
    entity_type: list[str] | None = None,
    had_primary_source: list[str] | None = None,
    skip: int = 0,
    limit: int = 100,
    validation: Literal[Validation.LENIENT] = Validation.LENIENT,
) -> PaginatedItemsContainer[AnyPreviewModel]: ...


@overload
def search_merged_items_in_graph(
    query_string: str | None = None,
    identifier: str | None = None,
    entity_type: list[str] | None = None,
    had_primary_source: list[str] | None = None,
    skip: int = 0,
    limit: int = 100,
    validation: Literal[Validation.STRICT] = Validation.STRICT,
) -> PaginatedItemsContainer[AnyMergedModel]: ...


@overload
def search_merged_items_in_graph(
    query_string: str | None = None,
    identifier: str | None = None,
    entity_type: list[str] | None = None,
    had_primary_source: list[str] | None = None,
    skip: int = 0,
    limit: int = 100,
    validation: Literal[Validation.IGNORE] = Validation.IGNORE,
) -> PaginatedItemsContainer[AnyMergedModel]: ...


def search_merged_items_in_graph(  # noqa: PLR0913
    query_string: str | None = None,
    identifier: str | None = None,
    entity_type: list[str] | None = None,
    had_primary_source: list[str] | None = None,
    skip: int = 0,
    limit: int = 100,
    validation: Literal[
        Validation.STRICT, Validation.LENIENT, Validation.IGNORE
    ] = Validation.STRICT,
) -> PaginatedItemsContainer[AnyPreviewModel] | PaginatedItemsContainer[AnyMergedModel]:
    """Search for merged items.

    Args:
        query_string: Full text search query term
        identifier: Optional merged item identifier filter
        entity_type: Optional entity type filter
        had_primary_source: optional merged primary source identifier filter
        skip: How many items to skip for pagination
        limit: How many items to return at most
        validation: Merged items validate the existence of required fields and
            the lengths of lists by default, set this to `LENIENT` to avoid this and
            return a "preview" of a merged item instead of a valid merged item,
            or set this to `IGNORE` to return None in case of validation errors.

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
    total = int(result["total"])
    items = [merge_search_result_item(item, validation) for item in result["items"]]
    if validation == Validation.LENIENT:
        return PaginatedItemsContainer[AnyPreviewModel](items=items, total=total)
    if validation == Validation.IGNORE:
        raise NotImplementedError
    return PaginatedItemsContainer[AnyMergedModel](items=items, total=total)
