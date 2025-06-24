from collections.abc import Sequence
from typing import Annotated

from fastapi import APIRouter, Query
from fastapi.exceptions import HTTPException
from starlette import status

from mex.backend.extracted.helpers import (
    get_extracted_item_from_graph,
    search_extracted_items_in_graph,
)
from mex.backend.graph.exceptions import NoResultFoundError
from mex.backend.types import ExtractedType
from mex.common.models import AnyExtractedModel, PaginatedItemsContainer
from mex.common.types import Identifier

router = APIRouter()


@router.get("/extracted-item", tags=["editor"])
def search_extracted_items(  # noqa: PLR0913
    q: Annotated[str, Query(max_length=100)] = "",
    stableTargetId: Annotated[Identifier | None, Query()] = None,
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


@router.get("/extracted-item/{identifier}", tags=["editor"])
def get_extracted_item(identifier: Identifier) -> AnyExtractedModel:
    """Return one extracted item for the given `identifier`."""
    try:
        return get_extracted_item_from_graph(identifier)
    except NoResultFoundError as error:
        raise HTTPException(status.HTTP_404_NOT_FOUND) from error
