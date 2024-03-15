from typing import Annotated, Sequence

from fastapi import APIRouter, Query

from mex.backend.extracted.models import ExtractedItemSearchResponse, ExtractedType
from mex.backend.extracted.transform import (
    transform_graph_results_to_extracted_item_search_response,
)
from mex.backend.graph.connector import GraphConnector
from mex.common.types import Identifier

router = APIRouter()


@router.get("/extracted-item", tags=["editor"])
def search_extracted_items(
    q: Annotated[str, Query(max_length=100)] = "",
    stableTargetId: Identifier | None = None,  # noqa: N803
    entityType: Annotated[  # noqa: N803
        Sequence[ExtractedType], Query(max_length=len(ExtractedType))
    ] = [],
    skip: Annotated[int, Query(ge=0, le=10e10)] = 0,
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
) -> ExtractedItemSearchResponse:
    """Search for extracted items by query text or by type and id."""
    graph = GraphConnector.get()
    query_results = graph.query_nodes(
        q,
        stableTargetId,
        [str(t.value) for t in entityType or ExtractedType],
        skip,
        limit,
    )
    return transform_graph_results_to_extracted_item_search_response(query_results)
