from pydantic import ValidationError

from mex.backend.extracted.models import ExtractedItemSearch
from mex.backend.graph.connector import GraphConnector
from mex.backend.graph.exceptions import InconsistentGraphError
from mex.common.models import AnyExtractedModel


def search_extracted_items_in_graph(
    query_string: str | None = None,
    stable_target_id: str | None = None,
    entity_type: list[str] | None = None,
    skip: int = 0,
    limit: int = 100,
) -> ExtractedItemSearch:
    """Search for extracted items in the graph.

    Args:
        query_string: Full text search query term
        stable_target_id: Optional stable target ID filter
        entity_type: Optional entity type filter
        skip: How many items to skip for pagination
        limit: How many items to return at most

    Raises:
        InconsistentGraphError: When the graph response cannot be parsed

    Returns:
        ExtractedItemSearch instance
    """
    connector = GraphConnector.get()
    graph_result = connector.fetch_extracted_items(
        query_string=query_string,
        stable_target_id=stable_target_id,
        entity_type=entity_type,
        skip=skip,
        limit=limit,
    )
    try:
        return ExtractedItemSearch.model_validate(graph_result.one())
    except ValidationError as error:
        raise InconsistentGraphError from error


def get_extracted_items_from_graph(
    stable_target_id: str | None = None,
    entity_type: list[str] | None = None,
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
