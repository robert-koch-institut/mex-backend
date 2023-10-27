from typing import Sequence

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from mex.backend.extracted.models import ExtractedType
from mex.backend.graph.connector import GraphConnector
from mex.backend.merged.models import MergedItemSearchResponse
from mex.backend.merged.transform import (
    transform_graph_results_to_merged_item_search_response_facade,
)
from mex.backend.transform import to_primitive
from mex.common.types import Identifier

router = APIRouter()


@router.get("/merged-item", tags=["editor"])
def search_merged_items_facade(
    q: str = Query("", max_length=1000),
    stableTargetId: Identifier | None = Query(None),  # noqa: N803
    entityType: Sequence[ExtractedType] = Query([]),  # noqa: N803
    skip: int = Query(0, ge=0, le=10e10),
    limit: int = Query(10, ge=1, le=100),
) -> MergedItemSearchResponse:
    """Facade for retrieving merged items."""
    # XXX We just search for extracted items and pretend they are already merged
    #     as a stopgap for MX-1382.
    graph = GraphConnector.get()
    query_results = graph.query_nodes(
        q,
        stableTargetId,
        [t.value for t in entityType or ExtractedType],
        skip,
        limit,
    )
    response = transform_graph_results_to_merged_item_search_response_facade(
        query_results
    )
    return JSONResponse(to_primitive(response), 200)  # type: ignore[return-value]
