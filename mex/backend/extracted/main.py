from collections.abc import Sequence
from typing import Annotated

from fastapi import APIRouter, Body, Depends, Path, Query
from fastapi.exceptions import HTTPException
from starlette import status

from mex.backend.extracted.helpers import (
    get_extracted_item_from_graph,
    search_extracted_items_in_graph,
)
from mex.backend.graph.exceptions import NoResultFoundError
from mex.backend.helpers import build_reference_filters
from mex.backend.models import ReferenceFilter
from mex.backend.security import has_read_access
from mex.backend.types import ExtractedType, ReferenceFieldName
from mex.common.models import AnyExtractedModel, PaginatedItemsContainer
from mex.common.types import Identifier

router = APIRouter()


@router.get("/extracted-item", tags=["editor"], dependencies=[Depends(has_read_access)])
def search_extracted_items(  # noqa: PLR0913
    q: Annotated[str, Query(max_length=100)] = "",
    stableTargetId: Annotated[
        Identifier | None,
        Query(
            deprecated=True,
            description="Use /extracted-item/_search with reference filters instead.",
        ),
    ] = None,
    entityType: Annotated[
        Sequence[ExtractedType], Query(max_length=len(ExtractedType))
    ] = [],
    referencedIdentifier: Annotated[
        Sequence[Identifier] | None,
        Query(
            max_length=100,
            deprecated=True,
            description="Use /extracted-item/_search with reference filters instead.",
        ),
    ] = None,
    referenceField: Annotated[
        ReferenceFieldName | None,
        Query(
            deprecated=True,
            description="Use /extracted-item/_search with reference filters instead.",
        ),
    ] = None,
    skip: Annotated[int, Query(ge=0, le=10e10)] = 0,
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
) -> PaginatedItemsContainer[AnyExtractedModel]:
    """Search for extracted items using simple filters.

    For complex queries combining multiple reference filters, use POST
    /extracted-item/_search
    """
    reference_filters = build_reference_filters(
        referenceField, referencedIdentifier, stableTargetId
    )

    return search_extracted_items_in_graph(
        query_string=q,
        entity_type=[str(t.value) for t in entityType or ExtractedType],
        reference_filters=reference_filters,
        skip=skip,
        limit=limit,
    )


@router.post(
    "/extracted-item/_search", tags=["editor"], dependencies=[Depends(has_read_access)]
)
def search_extracted_items_advanced(
    q: Annotated[str, Body(max_length=100)] = "",
    entityType: Annotated[
        Sequence[ExtractedType], Body(max_length=len(ExtractedType))
    ] = [],
    referenceFilters: Annotated[Sequence[ReferenceFilter], Body(max_length=100)]
    | None = None,
    skip: Annotated[int, Body(ge=0, le=10e10)] = 0,
    limit: Annotated[int, Body(ge=1, le=100)] = 10,
) -> PaginatedItemsContainer[AnyExtractedModel]:
    """Search for extracted items with advanced filter combinations.

    Use this endpoint for:
    - Multiple reference filters combined with AND logic, e.g. hadPrimarySource AND
      unitOf
    """
    return search_extracted_items_in_graph(
        query_string=q,
        entity_type=[str(t.value) for t in entityType or ExtractedType],
        reference_filters=referenceFilters,
        skip=skip,
        limit=limit,
    )


@router.get(
    "/extracted-item/{identifier}",
    tags=["editor"],
    dependencies=[Depends(has_read_access)],
)
def get_extracted_item(identifier: Annotated[Identifier, Path()]) -> AnyExtractedModel:
    """Return one extracted item for the given `identifier`."""
    try:
        return get_extracted_item_from_graph(identifier)
    except NoResultFoundError as error:
        raise HTTPException(status.HTTP_404_NOT_FOUND) from error
