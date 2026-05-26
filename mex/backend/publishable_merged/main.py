from collections.abc import Sequence
from typing import Annotated

from fastapi import APIRouter, Depends, Query

from mex.backend.helpers import build_reference_filters
from mex.backend.publishable_merged.helpers import (
    search_publishable_merged_items_in_graph,
)
from mex.backend.security import has_read_access
from mex.backend.types import MergedType, ReferenceFieldName
from mex.common.models import (
    AnyMergedModel,
    PaginatedItemsContainer,
)
from mex.common.types import Identifier, PublishingTarget

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
        Sequence[Identifier] | None,
        Query(
            max_length=100,
            deprecated=True,
            description="Use /publishable-merged-item/_search with reference filters "
            "instead.",
        ),
    ] = None,
    referenceField: Annotated[
        ReferenceFieldName | None,
        Query(
            deprecated=True,
            description="Use /publishable-merged-item/_search with reference filters "
            "instead.",
        ),
    ] = None,
    skip: Annotated[int, Query(ge=0, le=10e10)] = 0,
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
) -> PaginatedItemsContainer[AnyMergedModel]:
    """Search for publishable merged items using simple filters.

    For complex queries combining multiple reference filters, use POST
    /publishable-merged-item/_search
    """
    reference_filters = build_reference_filters(referenceField, referencedIdentifier)
    return search_publishable_merged_items_in_graph(
        query_string=q,
        identifier=identifier,
        entity_type=[str(t.value) for t in entityType or MergedType],
        reference_filters=reference_filters,
        skip=skip,
        limit=limit,
        publishing_target=publishingTarget,
    )
