from mex.backend.extracted.models import ExtractedItemSearch
from mex.backend.graph.connector import GraphConnector


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
    search_result = graph_result.one()
    return ExtractedItemSearch.model_validate(search_result)
