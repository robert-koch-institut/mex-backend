from collections.abc import Sequence
from typing import Annotated

from fastapi import APIRouter, Query

from mex.backend.extracted.helpers import search_extracted_items_in_graph
from mex.backend.types import ExtractedType
from mex.common.models import AnyExtractedModel, PaginatedItemsContainer
from mex.common.types import Identifier

router = APIRouter()


@router.get("/extracted-item", tags=["editor"])
def search_extracted_items(  # noqa: PLR0913
    q: Annotated[str, Query(max_length=100)] = "",
    stableTargetId: Identifier | None = None,
    entityType: Annotated[
        Sequence[ExtractedType], Query(max_length=len(ExtractedType))
    ] = [],
    hadPrimarySource: Annotated[Sequence[Identifier] | None, Query()] = None,
    skip: Annotated[int, Query(ge=0, le=10e10)] = 0,
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
) -> PaginatedItemsContainer[AnyExtractedModel]:
    """Search for extracted items by query text or by type and id."""
    return search_extracted_items_in_graph(
        q,
        stableTargetId,
        [str(t.value) for t in entityType or ExtractedType],
        hadPrimarySource,
        skip,
        limit,
    )
