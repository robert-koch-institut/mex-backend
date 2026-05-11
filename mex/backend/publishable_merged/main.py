from collections.abc import Sequence
from typing import Annotated

from fastapi import APIRouter, Depends, Query
from fastapi.exceptions import HTTPException
from starlette import status

from mex.backend.publishable_merged.helpers import (
    search_publishable_merged_items_in_graph,
)
from mex.backend.security import has_read_access
from mex.backend.types import MergedType, ReferenceFieldName
from mex.common.models import (
    AnyMergedModel,
    PaginatedItemsContainer,
)
from mex.common.types import Identifier, PublishingTarget, Validation

router = APIRouter()


@router.get(
    "/publishable-merged-item", tags=["editor"], dependencies=[Depends(has_read_access)]
)
def search_publishable_merged_items(  # noqa: PLR0913
    publishingTarget: PublishingTarget,
    q: Annotated[str, Query(max_length=100)] = "",
    identifier: Annotated[Identifier | None, Query()] = None,
    entityType: Annotated[Sequence[MergedType], Query(max_length=len(MergedType))] = [],
    referencedIdentifier: Annotated[
        Sequence[Identifier] | None, Query(max_length=100, deprecated=True)
    ] = None,
    referenceField: Annotated[ReferenceFieldName | None, Query(deprecated=True)] = None,
    skip: Annotated[int, Query(ge=0, le=10e10)] = 0,
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
) -> PaginatedItemsContainer[AnyMergedModel]:
    """Search for merged items by query text or by type and identifier."""
    if bool(referencedIdentifier) != bool(referenceField):
        msg = "Must provide referencedIdentifier AND referenceField or neither."
        raise HTTPException(status.HTTP_400_BAD_REQUEST, msg)
    return search_publishable_merged_items_in_graph(
        query_string=q,
        identifier=identifier,
        entity_type=[str(t.value) for t in entityType or MergedType],
        referenced_identifiers=[str(s) for s in referencedIdentifier]
        if referencedIdentifier
        else None,
        reference_field=str(referenceField.value) if referenceField else None,
        skip=skip,
        limit=limit,
        validation=Validation.IGNORE,
        publishing_target=publishingTarget,
    )
