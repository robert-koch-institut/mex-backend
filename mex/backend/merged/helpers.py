from typing import TYPE_CHECKING, Literal, overload

from mex.backend.graph.connector import GraphConnector
from mex.backend.graph.exceptions import (
    InconsistentGraphError,
    NoResultFoundError,
)
from mex.backend.merged.transform import merge_search_result_item
from mex.common.logging import logger
from mex.common.models import (
    AnyMergedModel,
    AnyPreviewModel,
    PaginatedItemsContainer,
)
from mex.common.types import Identifier, Validation

if TYPE_CHECKING:
    from collections.abc import Sequence

    from mex.backend.models import ReferenceFilter


@overload
def search_merged_items_in_graph(
    *,
    query_string: str | None = None,
    identifier: str | None = None,
    entity_type: Sequence[str] | None = None,
    reference_filters: Sequence[ReferenceFilter] | None = None,
    skip: int = 0,
    limit: int = 100,
    validation: Literal[Validation.LENIENT] = Validation.LENIENT,
) -> PaginatedItemsContainer[AnyPreviewModel]: ...


@overload
def search_merged_items_in_graph(
    *,
    query_string: str | None = None,
    identifier: str | None = None,
    entity_type: Sequence[str] | None = None,
    reference_filters: Sequence[ReferenceFilter] | None = None,
    skip: int = 0,
    limit: int = 100,
    validation: Literal[Validation.STRICT] = Validation.STRICT,
) -> PaginatedItemsContainer[AnyMergedModel]: ...


@overload
def search_merged_items_in_graph(
    *,
    query_string: str | None = None,
    identifier: str | None = None,
    entity_type: Sequence[str] | None = None,
    reference_filters: Sequence[ReferenceFilter] | None = None,
    skip: int = 0,
    limit: int = 100,
    validation: Literal[Validation.IGNORE] = Validation.IGNORE,
) -> PaginatedItemsContainer[AnyMergedModel]: ...


def search_merged_items_in_graph(  # noqa: PLR0913
    *,
    query_string: str | None = None,
    identifier: str | None = None,
    entity_type: Sequence[str] | None = None,
    reference_filters: Sequence[ReferenceFilter] | None = None,
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
        reference_filters: Optional reference field filters
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
        reference_filters=reference_filters,
        skip=skip,
        limit=limit,
    )
    total = int(result["total"])
    items = [
        merged_model
        for item in result["items"]
        if (merged_model := merge_search_result_item(item, validation))
    ]
    if validation == Validation.LENIENT:
        return PaginatedItemsContainer[AnyPreviewModel](items=items, total=total)
    return PaginatedItemsContainer[AnyMergedModel](items=items, total=total)


def get_merged_item_from_graph(identifier: Identifier) -> AnyMergedModel:
    """Fetch and return the merged item for the given `identifier`."""
    connector = GraphConnector.get()
    result = connector.fetch_merged_items(
        query_string=None,
        identifier=identifier,
        entity_type=None,
        reference_filters=None,
        skip=0,
        limit=1,
    )
    if result["total"] == 0:
        msg = "Merged item was not found."
        raise NoResultFoundError(msg)
    if result["total"] != 1:
        msg = "Found multiple merged items."
        raise InconsistentGraphError(msg)
    return merge_search_result_item(result["items"][0], Validation.STRICT)


def delete_merged_item_from_graph(identifier: Identifier) -> None:
    """Delete a merged item including all extracted items and rule-sets."""
    connector = GraphConnector.get()
    result = connector.delete_item(identifier)
    if result["deleted_merged_count"] == 0:
        msg = "Merged item was not found."
        raise NoResultFoundError(msg)
    logger.info("deleted item %s: %s", identifier, result.one())


def merge_items_in_graph(
    goner_identifier: Identifier,
    keeper_identifier: Identifier,
) -> None:
    """Merge a goner merged item into a keeper merged item in the graph."""
    connector = GraphConnector.get()
    goner = get_merged_item_from_graph(goner_identifier)
    keeper = get_merged_item_from_graph(keeper_identifier)
    connector.merge_items(goner, keeper)
