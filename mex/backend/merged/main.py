from collections.abc import Sequence
from typing import Annotated

from fastapi import APIRouter, Query

from mex.backend.merged.helpers import search_merged_items_in_graph
from mex.backend.merged.models import MergedItemSearch
from mex.backend.types import MergedType
from mex.common.types import Identifier

router = APIRouter()


@router.get("/merged-item", tags=["editor", "public"])
def search_merged_items(
    q: Annotated[str, Query(max_length=100)] = "",
    identifier: Identifier | None = None,
    entityType: Annotated[Sequence[MergedType], Query(max_length=len(MergedType))] = [],
    skip: Annotated[int, Query(ge=0, le=10e10)] = 0,
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
) -> MergedItemSearch:
    """Search for merged items by query text or by type and id."""
    return search_merged_items_in_graph(
        q,
        identifier,
        [str(t.value) for t in entityType or MergedType],
        skip,
        limit,
    )
