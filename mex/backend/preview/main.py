from typing import Annotated, Any

from fastapi import APIRouter, HTTPException, status
from fastapi.responses import JSONResponse
from pydantic import Field

from mex.backend.extracted.models import ExtractedItemSearchResponse
from mex.backend.fields import MERGEABLE_FIELDS_BY_CLASS_NAME
from mex.backend.graph.connector import GraphConnector
from mex.backend.transform import to_primitive
from mex.backend.utils import extend_list_in_dict, prune_list_in_dict
from mex.common.models import (
    ADDITIVE_MODEL_CLASSES_BY_NAME,
    MERGED_MODEL_CLASSES_BY_NAME,
    SUBTRACTIVE_MODEL_CLASSES_BY_NAME,
    AnyAdditiveModel,
    AnyMergedModel,
    AnySubtractiveModel,
)
from mex.common.transform import ensure_prefix
from mex.common.types import Identifier

router = APIRouter()


@router.post("/preview-item/{stableTargetId}", tags=["editor"])
def preview_item(
    stableTargetId: Identifier,
    rule: Annotated[
        AnyAdditiveModel | AnySubtractiveModel, Field(discriminator="entityType")
    ],
) -> AnyMergedModel:
    """Preview the merging result when the given rule would be applied."""
    connector = GraphConnector.get()
    graph_result = connector.fetch_extracted_data(
        query_string=None,
        stable_target_id=stableTargetId,
        entity_type=None,
        skip=0,
        limit=100,
    )
    extracted_items = ExtractedItemSearchResponse.model_validate(
        graph_result.one()
    ).items
    if not extracted_items:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="stableTargetId not found in database",
        )
    if rule.stemType not in {item.stemType for item in extracted_items}:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"stableTargetId is not of type '{rule.stemType}'",
        )

    # TODO: fetch existing rules from database and merge them as well

    mergeable_fields = MERGEABLE_FIELDS_BY_CLASS_NAME[rule.entityType]
    merged_model_name = ensure_prefix(rule.stemType, "Merged")
    merged_model_class = MERGED_MODEL_CLASSES_BY_NAME[merged_model_name]
    merged_dict: dict[str, Any] = {"identifier": stableTargetId}

    # merge extracted items
    for extracted_item in extracted_items:
        for field_name in mergeable_fields:
            extracted_value = getattr(extracted_item, field_name)
            extend_list_in_dict(merged_dict, field_name, extracted_value)

    if rule.entityType in ADDITIVE_MODEL_CLASSES_BY_NAME:
        # merge additive rule
        for field_name in mergeable_fields:
            rule_value = getattr(rule, field_name)
            extend_list_in_dict(merged_dict, field_name, rule_value)

    elif rule.entityType in SUBTRACTIVE_MODEL_CLASSES_BY_NAME:
        # merge subtractive rule
        for field_name in mergeable_fields:
            rule_value = getattr(rule, field_name)
            prune_list_in_dict(merged_dict, field_name, rule_value)

    return JSONResponse(  # type: ignore[return-value]
        to_primitive(merged_model_class.model_validate(merged_dict))
    )
