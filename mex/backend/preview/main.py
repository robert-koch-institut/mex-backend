from collections.abc import Sequence
from typing import Annotated

from fastapi import APIRouter, Body, HTTPException, Path, Query
from starlette import status

from mex.backend.extracted.helpers import get_extracted_items_from_graph
from mex.backend.merged.helpers import search_merged_items_in_graph
from mex.backend.types import MergedType, ReferenceFieldName
from mex.common.merged.main import create_merged_item_for_publishing_target
from mex.common.models import (
    AnyMergedModel,
    AnyPreviewModel,
    AnyRuleSetRequest,
    PaginatedItemsContainer,
)
from mex.common.transform import ensure_prefix
from mex.common.types import Identifier, PublishingTarget, Validation

router = APIRouter()


@router.get("/preview-item", tags=["editor"])
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
