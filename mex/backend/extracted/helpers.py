from collections.abc import Sequence

from pydantic_core import ValidationError

from mex.backend.exceptions import BackendError
from mex.backend.graph.connector import GraphConnector
from mex.backend.graph.exceptions import InconsistentGraphError
from mex.common.models import (
    AnyExtractedModel,
    ExtractedModelTypeAdapter,
    PaginatedItemsContainer,
)
from mex.common.types import Identifier


def search_extracted_items_in_graph(  # noqa: PLR0913
    query_string: str | None = None,
    stable_target_id: Identifier | None = None,
    entity_type: Sequence[str] | None = None,
    had_primary_source: Sequence[str] | None = None,
    skip: int = 0,
    limit: int = 100,
) -> PaginatedItemsContainer[AnyExtractedModel]:
    """Search for extracted items in the graph.

    Args:
        query_string: Full text search query term
        stable_target_id: Optional stable target ID filter
        entity_type: Optional entity type filter
        had_primary_source: Optional merged primary source identifier filter
        skip: How many items to skip for pagination
        limit: How many items to return at most

    Raises:
        InconsistentGraphError: When the graph response cannot be parsed

    Returns:
        Paginated list of extracted items
    """
    connector = GraphConnector.get()
    graph_result = connector.fetch_extracted_items(
        query_string,
        None,
        stable_target_id,
        entity_type,
        had_primary_source,
        skip,
        limit,
    )
    search_result = graph_result.one()
    try:
        return PaginatedItemsContainer[AnyExtractedModel].model_validate(search_result)
    except ValidationError as error:
        raise InconsistentGraphError from error


def get_extracted_items_from_graph(
    stable_target_id: Identifier | None = None,
    entity_type: Sequence[str] | None = None,
    limit: int = 100,
) -> list[AnyExtractedModel]:
    """Get a list of extracted items for the given id and type.

    Args:
        stable_target_id: Optional stable target ID filter
        entity_type: Optional entity type filter
        limit: How many items to return at most

    Raises:
        InconsistentGraphError: When the graph response cannot be parsed

    Returns:
        List of extracted items
    """
    search_response = search_extracted_items_in_graph(
        stable_target_id=stable_target_id,
        entity_type=entity_type,
        limit=limit,
    )
    return search_response.items


def get_extracted_item_from_graph(identifier: Identifier) -> AnyExtractedModel:
    """Fetch and return the extracted item for the given `identifier`."""
    connector = GraphConnector.get()
    graph_result = connector.fetch_extracted_items(
        query_string=None,
        identifier=identifier,
        stable_target_id=None,
        entity_type=None,
        had_primary_source=None,
        skip=0,
        limit=1,
    )
    result = graph_result.one()
    if not int(result["total"]):
        msg = "Merged item was not found."
        raise BackendError(msg) from None
    return ExtractedModelTypeAdapter.validate_python(result)
