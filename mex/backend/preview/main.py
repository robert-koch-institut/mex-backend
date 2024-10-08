from fastapi import APIRouter

from mex.backend.extracted.helpers import get_extracted_items_from_graph
from mex.backend.merged.helpers import create_merged_item
from mex.common.models import AnyMergedModel, AnyRuleSetRequest
from mex.common.transform import ensure_prefix
from mex.common.types import Identifier

router = APIRouter()


@router.post("/preview-item/{stableTargetId}", tags=["editor"])
def preview_item(
    stableTargetId: Identifier,
    ruleSet: AnyRuleSetRequest,
) -> AnyMergedModel:
    """Preview the merging result when the given rule would be applied."""
    extracted_items = get_extracted_items_from_graph(
        stable_target_id=stableTargetId,
        entity_type=[ensure_prefix(ruleSet.stemType, "Extracted")],
    )
    return create_merged_item(stableTargetId, extracted_items, ruleSet)
