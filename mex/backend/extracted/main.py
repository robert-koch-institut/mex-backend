from typing import Sequence

from fastapi import APIRouter, Query

from mex.backend.extracted.models import ExtractedItemSearchResponse
from mex.backend.graph.connector import GraphConnector
from mex.backend.types import ExtractedType
from mex.common.types import Identifier

router = APIRouter()


@router.get("/extracted-item", tags=["editor"])
def search_extracted_items(
    q: str = Query("", max_length=1000),
    stableTargetId: Identifier | None = Query(None),  # noqa: N803
    entityType: Sequence[ExtractedType] = Query([]),  # noqa: N803
    skip: int = Query(0, ge=0, le=10e10),
    limit: int = Query(10, ge=1, le=100),
) -> ExtractedItemSearchResponse:
    """Search for extracted items by query text or by type and id."""
    graph = GraphConnector.get()
    result = graph.fetch_extracted_data(
        q,
        stableTargetId,
        [str(t.value) for t in entityType or ExtractedType],
        skip,
        limit,
    )
    return ExtractedItemSearchResponse.model_validate(result.one())
