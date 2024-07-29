from collections.abc import Sequence
from typing import Annotated

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from mex.backend.extracted.models import ExtractedItemSearchResponse
from mex.backend.graph.connector import GraphConnector
from mex.backend.transform import to_primitive
from mex.backend.types import ExtractedType
from mex.common.types import Identifier

router = APIRouter()


@router.get("/extracted-item", tags=["editor"])
def search_extracted_items(
    q: Annotated[str, Query(max_length=100)] = "",
    stableTargetId: Identifier | None = None,
    entityType: Annotated[
        Sequence[ExtractedType], Query(max_length=len(ExtractedType))
    ] = [],
    skip: Annotated[int, Query(ge=0, le=10e10)] = 0,
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
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
    response = ExtractedItemSearchResponse.model_validate(result.one())
    return JSONResponse(to_primitive(response))  # type: ignore[return-value]
