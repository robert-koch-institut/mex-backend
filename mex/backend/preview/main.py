from typing import Annotated, Any

from fastapi import APIRouter, HTTPException, status
from pydantic import PlainSerializer

from mex.backend.extracted.models import ExtractedItemSearchResponse
from mex.backend.fields import MERGEABLE_FIELDS_BY_CLASS_NAME
from mex.backend.graph.connector import GraphConnector
from mex.backend.preview.models import RuleSet
from mex.backend.transform import to_primitive
from mex.backend.utils import extend_list_in_dict, prune_list_in_dict
from mex.common.models import MERGED_MODEL_CLASSES_BY_NAME, AnyMergedModel
from mex.common.transform import ensure_prefix
from mex.common.types import Identifier

router = APIRouter()


@router.post("/preview-item/{stableTargetId}", tags=["editor"])
def preview_item(
    stableTargetId: Identifier,
    ruleSet: RuleSet,
) -> Annotated[AnyMergedModel, PlainSerializer(to_primitive)]:
    """Preview the merging result when the given rule would be applied."""
    connector = GraphConnector.get()
    graph_result = connector.fetch_extracted_data(
        query_string=None,
        stable_target_id=stableTargetId,
        entity_type=[ensure_prefix(ruleSet.additive.stemType, "Extracted")],
        skip=0,
        limit=100,
    )
    extracted_items = ExtractedItemSearchResponse.model_validate(
        graph_result.one()
    ).items
    if not extracted_items:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No extracted items found in database",
        )

    merged_model_name = ensure_prefix(ruleSet.additive.stemType, "Merged")
    mergeable_fields = MERGEABLE_FIELDS_BY_CLASS_NAME[merged_model_name]
    merged_model_class = MERGED_MODEL_CLASSES_BY_NAME[merged_model_name]
    merged_dict: dict[str, Any] = {"identifier": stableTargetId}

    # merge extracted items
    for extracted_item in extracted_items:
        for field_name in mergeable_fields:
            # apply preventive rule
            if extracted_item.hadPrimarySource in getattr(
                ruleSet.preventive, field_name
            ):
                continue
            extracted_value = getattr(extracted_item, field_name)
            extend_list_in_dict(merged_dict, field_name, extracted_value)

    # merge additive rule
    for field_name in mergeable_fields:
        rule_value = getattr(ruleSet.additive, field_name)
        extend_list_in_dict(merged_dict, field_name, rule_value)

    # merge subtractive rule
    for field_name in mergeable_fields:
        rule_value = getattr(ruleSet.subtractive, field_name)
        prune_list_in_dict(merged_dict, field_name, rule_value)

    return merged_model_class.model_validate(merged_dict)
