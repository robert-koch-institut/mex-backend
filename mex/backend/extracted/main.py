from collections.abc import Sequence
from typing import Annotated

from fastapi import APIRouter, Body, Path, Query
from fastapi.exceptions import HTTPException
from starlette import status

from mex.backend.extracted.helpers import (
    get_extracted_item_from_graph,
    search_extracted_items_in_graph,
)
from mex.backend.graph.exceptions import NoResultFoundError
from mex.backend.models import ReferenceFilter
from mex.backend.types import ExtractedType, ReferenceFieldName
from mex.common.models import AnyExtractedModel, PaginatedItemsContainer
from mex.common.types import Identifier

router = APIRouter()


@router.get("/extracted-item", tags=["editor"])
def search_extracted_items(  # noqa: PLR0913
    q: Annotated[str, Query(max_length=100)] = "",
    stableTargetId: Annotated[Identifier, Query()] | None = None,
    entityType: Annotated[
        Sequence[ExtractedType], Query(max_length=len(ExtractedType))
    ] = [],
    referencedIdentifier: Annotated[
        Sequence[Identifier], Query(max_length=100, deprecated=True)
    ]
    | None = None,
    referenceField: Annotated[ReferenceFieldName, Query(deprecated=True)] | None = None,
    skip: Annotated[int, Query(ge=0, le=10e10)] = 0,
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
) -> PaginatedItemsContainer[AnyExtractedModel]:
    """Search for extracted items by query text or by type and id."""
    reference_filters: Sequence[ReferenceFilter] | None
    if not referencedIdentifier and not referenceField:
        reference_filters = None
    elif referencedIdentifier and referenceField:
        reference_filters = [
            ReferenceFilter(field=referenceField, identifiers=referencedIdentifier)
        ]
    else:
        msg = "Must provide referencedIdentifier AND referenceField or neither."
        raise HTTPException(status.HTTP_400_BAD_REQUEST, msg)
    return search_extracted_items_in_graph(
        query_string=q,
        stable_target_id=stableTargetId,
        entity_type=[str(t.value) for t in entityType or ExtractedType],
        reference_filters=reference_filters,
        skip=skip,
        limit=limit,
    )


@router.post("/extracted-item-search", tags=["editor"])
def search_extracted_items_advanced(  # noqa: PLR0913
    q: Annotated[str, Query(max_length=100)] = "",
    stableTargetId: Annotated[Identifier, Query()] | None = None,
    entityType: Annotated[
        Sequence[ExtractedType], Query(max_length=len(ExtractedType))
    ] = [],
    referenceFilters: Annotated[Sequence[ReferenceFilter], Body(max_length=100)]
    | None = None,
    skip: Annotated[int, Query(ge=0, le=10e10)] = 0,
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
) -> PaginatedItemsContainer[AnyExtractedModel]:
    """Search for extracted items with advanced search filters."""
    return search_extracted_items_in_graph(
        query_string=q,
        stable_target_id=stableTargetId,
        entity_type=[str(t.value) for t in entityType or ExtractedType],
        reference_filters=referenceFilters,
        skip=skip,
        limit=limit,
    )


@router.get("/extracted-item/{identifier}", tags=["editor"])
def get_extracted_item(identifier: Annotated[Identifier, Path()]) -> AnyExtractedModel:
    """Return one extracted item for the given `identifier`."""
    try:
        return get_extracted_item_from_graph(identifier)
    except NoResultFoundError as error:
        raise HTTPException(status.HTTP_404_NOT_FOUND) from error
