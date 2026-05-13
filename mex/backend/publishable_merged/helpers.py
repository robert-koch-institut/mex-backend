from typing import TYPE_CHECKING, Any

from pydantic import ValidationError

from mex.backend.graph.connector import GraphConnector
from mex.backend.rules.transform import transform_raw_rules_to_rule_set_response
from mex.common.exceptions import MergingError
from mex.common.merged.main import create_merged_item, is_item_publishable
from mex.common.models import (
    EXTRACTED_MODEL_CLASSES_BY_NAME,
    RULE_MODEL_CLASSES_BY_NAME,
    AnyMergedModel,
    ExtractedModelTypeAdapter,
    PaginatedItemsContainer,
)
from mex.common.types import Identifier, PublishingTarget, Validation

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Sequence


def merge_publishable_search_result_item(
    item: dict[str, Any],
    publishing_target: PublishingTarget,
) -> AnyMergedModel | None:
    """Merge a single search result into a merged item.

    Args:
        item: Raw merged search result item from the graph response
        validation: Merged items validate the existence of required fields and
            the lengths of lists by default, set this to `LENIENT` to avoid this and
            return a "preview" of a merged item instead of a valid merged item,
            or set this to `IGNORE` to return None in case of validation errors.
        publishing_target: Target to which the items are published.

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
        if is_item_publishable(rule_set, publishing_target):
            return create_merged_item(
                identifier=Identifier(item["identifier"]),
                extracted_items=extracted_items,
                rule_set=rule_set,
                validation=Validation.IGNORE,
            )
        return None  # noqa: TRY300
    except MergingError, ValidationError:
        return None


def search_publishable_merged_items_in_graph(  # noqa: PLR0913
    *,
    query_string: str | None = None,
    identifier: str | None = None,
    entity_type: Sequence[str] | None = None,
    referenced_identifiers: Sequence[str] | None = None,
    reference_field: str | None = None,
    skip: int = 0,
    limit: int = 100,
    publishing_target: PublishingTarget,
) -> PaginatedItemsContainer[AnyMergedModel]:
    """Search for merged items.

    Args:
        query_string: Full text search query term
        identifier: Optional merged item identifier filter
        entity_type: Optional entity type filter
        referenced_identifiers: Optional merged item identifiers filter
        reference_field: Optional field name to filter for
        skip: How many items to skip for pagination
        limit: How many items to return at most
        publishing_target: Target to which the items are published.

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
        referenced_identifiers=referenced_identifiers,
        reference_field=reference_field,
        skip=skip,
        limit=limit,
    )
    total = int(result["total"])
    items = [
        merged_model
        for item in result["items"]
        if (
            merged_model := merge_publishable_search_result_item(
                item, publishing_target
            )
        )
    ]
    return PaginatedItemsContainer[AnyMergedModel](items=items, total=total)
