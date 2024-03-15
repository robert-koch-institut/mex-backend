from typing import Annotated, Sequence

from fastapi import APIRouter, Query

from mex.backend.graph.connector import GraphConnector
from mex.backend.merged.models import (
    MergedItemSearchResponse,
    MergedType,
    UnprefixedType,
)
from mex.backend.merged.transform import (
    transform_graph_results_to_merged_item_search_response_facade,
)
from mex.common.types import Identifier

router = APIRouter()


@router.get("/merged-item", tags=["editor", "public"])
def search_merged_items_facade(
    q: Annotated[str, Query(max_length=100)] = "",
    stableTargetId: Annotated[Identifier | None, Query()] = None,  # noqa: N803
    entityType: Annotated[  # noqa: N803
        Sequence[MergedType | UnprefixedType], Query(max_length=len(MergedType))
    ] = [],
    skip: Annotated[int, Query(ge=0, le=10e10)] = 0,
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
) -> MergedItemSearchResponse:
    """Facade for retrieving merged items."""
    # XXX We just search for extracted items and pretend they are already merged
    #     as a stopgap for MX-1382.
    graph = GraphConnector.get()
    query_results = graph.query_nodes(
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
    return transform_graph_results_to_merged_item_search_response_facade(query_results)
