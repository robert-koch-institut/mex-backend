from collections.abc import Sequence

from fastapi import APIRouter, Query

from mex.backend.graph.connector import GraphConnector
from mex.backend.merged.models import MergedItemSearchResponse
from mex.backend.merged.transform import (
    transform_graph_result_to_merged_item_search_response_facade,
)
from mex.backend.types import MergedType, UnprefixedType
from mex.common.types import Identifier

router = APIRouter()


@router.get("/merged-item", tags=["editor", "public"])
def search_merged_items_facade(
    q: str = Query("", max_length=1000),
    stableTargetId: Identifier | None = Query(None),  # noqa: N803
    entityType: Sequence[MergedType | UnprefixedType] = Query([]),  # noqa: N803
    skip: int = Query(0, ge=0, le=10e10),
    limit: int = Query(10, ge=1, le=100),
) -> MergedItemSearchResponse:
    """Facade for retrieving merged items."""
    # XXX We just search for extracted items and pretend they are already merged
    #     as a stopgap for MX-1382.
    graph = GraphConnector.get()
    query_result = graph.query_nodes(
        q,
        stableTargetId,
        [
            # Allow 'MergedPerson' as well as 'Person' as an entityType for this
            # endpoint to keep compatibility with previous API clients.
            f"Extracted{t.value.removeprefix('Merged')}"
            for t in entityType or MergedType
        ],
        skip,
        limit,
    )
    return transform_graph_result_to_merged_item_search_response_facade(query_result)
