from collections.abc import Sequence
from typing import Annotated

from fastapi import APIRouter, Query
from starlette import status

from mex.backend.merged.helpers import search_merged_items_in_graph
from mex.backend.merged.models import MergeRequest
from mex.backend.types import MergedType, Validation
from mex.common.models import AnyMergedModel, PaginatedItemsContainer
from mex.common.types import Identifier

router = APIRouter()


@router.get("/merge", status_code=status.HTTP_204_NO_CONTENT, tags=["editor"])
def merge_item(request: MergeRequest) -> None:
    """Assign an extracted item to a new merged item."""


@router.get("/merged-item", tags=["editor"])
def search_merged_items(  # noqa: PLR0913
    q: Annotated[str, Query(max_length=100)] = "",
    identifier: Identifier | None = None,
    entityType: Annotated[Sequence[MergedType], Query(max_length=len(MergedType))] = [],
    hadPrimarySource: Annotated[Sequence[Identifier] | None, Query()] = None,
    skip: Annotated[int, Query(ge=0, le=10e10)] = 0,
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
) -> PaginatedItemsContainer[AnyMergedModel]:
    """Search for merged items by query text or by type and identifier."""
    return search_merged_items_in_graph(
        q,
        identifier,
        [str(t.value) for t in entityType or MergedType],
        [str(s) for s in hadPrimarySource] if hadPrimarySource else None,
        skip,
        limit,
        Validation.IGNORE,
    )
