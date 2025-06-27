from collections.abc import Sequence
from typing import Annotated

from fastapi import APIRouter, Query
from fastapi.exceptions import HTTPException
from starlette import status

from mex.backend.graph.exceptions import NoResultFoundError
from mex.backend.merged.helpers import (
    get_merged_item_from_graph,
    search_merged_items_in_graph,
)
from mex.backend.types import MergedType, ReferenceFieldName, Validation
from mex.common.models import AnyMergedModel, PaginatedItemsContainer
from mex.common.types import Identifier

router = APIRouter()


@router.get("/merged-item", tags=["editor"])
def search_merged_items(  # noqa: PLR0913
    q: Annotated[str, Query(max_length=100)] = "",
    identifier: Annotated[Identifier | None, Query()] = None,
    entityType: Annotated[Sequence[MergedType], Query(max_length=len(MergedType))] = [],
    hadPrimarySource: Annotated[
        Sequence[Identifier] | None, Query(deprecated=True)
    ] = None,
    referencedIdentifier: Annotated[Sequence[Identifier] | None, Query()] = None,
    referenceField: Annotated[ReferenceFieldName | None, Query()] = None,
    skip: Annotated[int, Query(ge=0, le=10e10)] = 0,
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
) -> PaginatedItemsContainer[AnyMergedModel]:
    """Search for merged items by query text or by type and identifier."""
    if hadPrimarySource:
        referencedIdentifier = hadPrimarySource  # noqa: N806
        referenceField = ReferenceFieldName("hadPrimarySource")  # noqa: N806
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
        validation=Validation.IGNORE,
    )


@router.get("/merged-item/{identifier}", tags=["editor"])
def get_merged_item(identifier: Identifier) -> AnyMergedModel:
    """Return one merged item for the given `identifier`."""
    try:
        return get_merged_item_from_graph(identifier)
    except NoResultFoundError as error:
        raise HTTPException(status.HTTP_404_NOT_FOUND) from error
