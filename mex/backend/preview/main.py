from typing import Annotated

from fastapi import APIRouter, HTTPException, status
from pydantic import PlainSerializer

from mex.backend.extracted.helpers import get_extracted_items_from_graph
from mex.backend.graph.exceptions import NoResultFoundError
from mex.backend.merged.helpers import create_merged_item
from mex.backend.serialization import to_primitive
from mex.common.models import AnyMergedModel, AnyRuleSetRequest
from mex.common.transform import ensure_prefix
from mex.common.types import Identifier

router = APIRouter()


@router.post("/preview-item/{stableTargetId}", tags=["editor"])
def preview_item(
    stableTargetId: Identifier,
    ruleSet: AnyRuleSetRequest,
) -> Annotated[AnyMergedModel, PlainSerializer(to_primitive)]:
    """Preview the merging result when the given rule would be applied."""
    try:
        extracted_items = get_extracted_items_from_graph(
            stable_target_id=stableTargetId,
            entity_type=[ensure_prefix(ruleSet.stemType, "Extracted")],
        )
    except NoResultFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No extracted items found in database",
        ) from None

    return create_merged_item(stableTargetId, extracted_items, ruleSet)
