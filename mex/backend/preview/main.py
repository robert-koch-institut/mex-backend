from collections.abc import Sequence
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from starlette import status

from mex.backend.merged.helpers import search_merged_items_in_graph
from mex.backend.security import has_read_access
from mex.backend.types import MergedType, ReferenceFieldName
from mex.common.models import (
    AnyPreviewModel,
    PaginatedItemsContainer,
)
from mex.common.types import Identifier, Validation

router = APIRouter()


@router.get(
    "/preview-item/{identifier}",
    tags=["editor"],
    dependencies=[Depends(has_read_access)],
)
def get_preview_item(
    identifier: Annotated[Identifier, Path()],
) -> AnyPreviewModel:
    """Get the preview for an item by its identifier."""
    result = search_merged_items_in_graph(
        identifier=identifier,
        validation=Validation.LENIENT,
        publishing_target=None,
    )
    if result.total == 0:
        raise HTTPException(status.HTTP_404_NOT_FOUND)
    return result.items[0]


@router.get("/preview-item", tags=["editor"], dependencies=[Depends(has_read_access)])
def search_preview_items(  # noqa: PLR0913
    q: Annotated[str, Query(max_length=100)] = "",
    identifier: Annotated[Identifier | None, Query()] = None,
    entityType: Annotated[Sequence[MergedType], Query(max_length=len(MergedType))] = [],
    referencedIdentifier: Annotated[
        Sequence[Identifier] | None, Query(deprecated=True)
    ] = None,
    referenceField: Annotated[ReferenceFieldName | None, Query(deprecated=True)] = None,
    skip: Annotated[int, Query(ge=0, le=10e10)] = 0,
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
) -> PaginatedItemsContainer[AnyPreviewModel]:
    """Search for merged item previews by query text or by type and identifier.

    In contrast to `/merged-item`, this endpoint does not validate the existence
    of required fields or the length restrictions of lists.
    """
    if bool(referencedIdentifier) != bool(referenceField):
        msg = "Must provide referencedIdentifier AND referenceField or neither."
        raise HTTPException(status.HTTP_400_BAD_REQUEST, msg)
    return search_merged_items_in_graph(
        query_string=q,
        identifier=identifier,
        entity_type=[str(t.value) for t in entityType or MergedType],
        referenced_identifiers=[str(s) for s in referencedIdentifier]
        if referencedIdentifier
        else None,
        reference_field=str(referenceField.value) if referenceField else None,
        skip=skip,
        limit=limit,
        validation=Validation.LENIENT,
        publishing_target=None,
    )
