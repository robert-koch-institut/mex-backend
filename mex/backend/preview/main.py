from collections.abc import Sequence
from typing import Annotated

from fastapi import APIRouter, Body, HTTPException, Path, Query
from starlette import status

from mex.backend.extracted.helpers import get_extracted_items_from_graph
from mex.backend.merged.helpers import search_merged_items_in_graph
from mex.backend.types import MergedType, ReferenceFieldName, Validation
from mex.common.merged.main import create_merged_item
from mex.common.models import (
    AnyMergedModel,
    AnyPreviewModel,
    AnyRuleSetRequest,
    PaginatedItemsContainer,
)
from mex.common.transform import ensure_prefix
from mex.common.types import Identifier

router = APIRouter()


@router.post("/preview-item/{stableTargetId}", tags=["editor"])
def preview_item(
    stableTargetId: Annotated[Identifier, Path()],
    ruleSet: Annotated[AnyRuleSetRequest, Body(discriminator="entityType")],
) -> AnyMergedModel:
    """Preview the merging result when the given rule would be applied."""
    # TODO(ND): Convert this endpoint to return previews instead of merged items.
    #           This will allow editor users to see the resulting item, even if
    #           cardinality validation failed for some fields.
    #           We need to include any validation error alongside the preview though.
    extracted_items = get_extracted_items_from_graph(
        stable_target_id=stableTargetId,
        entity_type=[ensure_prefix(ruleSet.stemType, "Extracted")],
    )
    return create_merged_item(
        stableTargetId,
        extracted_items,
        ruleSet,
        validate_cardinality=True,
    )


@router.get("/preview-item", tags=["editor"])
def preview_items(  # noqa: PLR0913
    q: Annotated[str, Query(max_length=100)] = "",
    identifier: Annotated[Identifier | None, Query()] = None,
    entityType: Annotated[Sequence[MergedType], Query(max_length=len(MergedType))] = [],
    hadPrimarySource: Annotated[
        Sequence[Identifier] | None, Query(deprecated=True)
    ] = None,
    referencedIdentifiers: Annotated[Sequence[Identifier] | None, Query()] = None,
    referenceFieldName: Annotated[ReferenceFieldName | None, Query()] = None,
    skip: Annotated[int, Query(ge=0, le=10e10)] = 0,
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
) -> PaginatedItemsContainer[AnyPreviewModel]:
    """Search for merged item previews by query text or by type and identifier.

    In contrast to `/merged-item`, this endpoint does not validate the existence
    of required fields or the length restrictions of lists.
    """
    if hadPrimarySource:
        referencedIdentifiers = hadPrimarySource  # noqa: N806
        referenceFieldName = ReferenceFieldName["hadPrimarySource"]  # noqa: N806

    if bool(referencedIdentifiers) != bool(referenceFieldName):
        msg = "must provide referencedIdentifiers AND referenceFieldName or neither."
        raise HTTPException(status.HTTP_400_BAD_REQUEST, msg)
    return search_merged_items_in_graph(
        q,
        identifier,
        [str(t.value) for t in entityType or MergedType],
        [str(s) for s in referencedIdentifiers] if referencedIdentifiers else None,
        str(referenceFieldName.value) if referenceFieldName else None,
        skip,
        limit,
        Validation.LENIENT,
    )
