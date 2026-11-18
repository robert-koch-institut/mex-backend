from typing import Any, Literal, overload

from pydantic import ValidationError

from mex.backend.rules.transform import transform_raw_rules_to_rule_set_response
from mex.common.exceptions import MergingError
from mex.common.merged.main import create_merged_item
from mex.common.models import (
    EXTRACTED_MODEL_CLASSES_BY_NAME,
    RULE_MODEL_CLASSES_BY_NAME,
    AnyMergedModel,
    AnyPreviewModel,
    ExtractedModelTypeAdapter,
)
from mex.common.types import Identifier, Validation


@overload
def merge_search_result_item(
    item: dict[str, Any],
    validation: Literal[Validation.LENIENT],
) -> AnyPreviewModel: ...


@overload
def merge_search_result_item(
    item: dict[str, Any],
    validation: Literal[Validation.STRICT],
) -> AnyMergedModel: ...


@overload
def merge_search_result_item(
    item: dict[str, Any],
    validation: Literal[Validation.IGNORE],
) -> AnyMergedModel | None: ...


def merge_search_result_item(
    item: dict[str, Any],
    validation: Literal[Validation.STRICT, Validation.LENIENT, Validation.IGNORE],
) -> AnyPreviewModel | AnyMergedModel | None:
    """Merge a single search result into a merged item.

    Args:
        item: Raw merged search result item from the graph response
        validation: Merged items validate the existence of required fields and
            the lengths of lists by default, set this to `LENIENT` to avoid this and
            return a "preview" of a merged item instead of a valid merged item,
            or set this to `IGNORE` to return None in case of validation errors.

    Raises:
        MergingError: When the given items cannot be merged

    Returns:
        Instance of a merged or preview item
    """
    extracted_items = [
        ExtractedModelTypeAdapter.validate_python(component)
        for component in item["_components"]
        if component["entityType"] in EXTRACTED_MODEL_CLASSES_BY_NAME
    ]
    raw_rules = [
        component
        for component in item["_components"]
        if component["entityType"] in RULE_MODEL_CLASSES_BY_NAME
    ]
    if raw_rules:
        rule_set = transform_raw_rules_to_rule_set_response(raw_rules)
    else:
        rule_set = None
    try:
        return create_merged_item(
            identifier=Identifier(item["identifier"]),
            extracted_items=extracted_items,
            rule_set=rule_set,
            validation=validation,
        )
    except MergingError, ValidationError:
        if validation == Validation.STRICT:
            raise
    return None
