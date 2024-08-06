from mex.backend.graph.connector import GraphConnector
from mex.backend.merged.models import MergedItemSearch
from mex.backend.types import MergedType


def search_merged_items_in_graph(
    query_string: str | None = None,
    stable_target_id: str | None = None,
    entity_type: list[str] | None = None,
    skip: int = 0,
    limit: int = 100,
) -> MergedItemSearch:
    """Facade for searching for merged items.

    Args:
        query_string: Full text search query term
        stable_target_id: Optional stable target ID filter
        entity_type: Optional entity type filter
        skip: How many items to skip for pagination
        limit: How many items to return at most

    Returns:
        MergedItemSearch instance
    """
    # XXX We just search for extracted items and pretend they are already merged
    #     as a stopgap for MX-1382.
    graph = GraphConnector.get()
    graph_result = graph.fetch_extracted_items(
        query_string=query_string,
        stable_target_id=stable_target_id,
        entity_type=[
            t.replace("Merged", "Extracted")
            for t in entity_type or [str(m.value) for m in MergedType]
        ],
        skip=skip,
        limit=limit,
    )
    search_result = graph_result.one()
    for item in search_result["items"]:
        del item["hadPrimarySource"]
        del item["identifierInPrimarySource"]
        item["identifier"] = item.pop("stableTargetId")
        item["entityType"] = item["entityType"].replace("Extracted", "Merged")
    return MergedItemSearch.model_validate(search_result)
