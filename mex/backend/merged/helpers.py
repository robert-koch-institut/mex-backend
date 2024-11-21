from typing import Annotated, Any, cast

from pydantic import Field, TypeAdapter, ValidationError

from mex.backend.constants import NUMBER_OF_RULE_TYPES
from mex.backend.fields import MERGEABLE_FIELDS_BY_CLASS_NAME
from mex.backend.graph.connector import GraphConnector
from mex.backend.graph.exceptions import InconsistentGraphError
from mex.backend.merged.models import MergedItemSearch
from mex.backend.rules.helpers import transform_raw_rules_to_rule_set_response
from mex.backend.utils import extend_list_in_dict, prune_list_in_dict, reraising
from mex.common.exceptions import MExError
from mex.common.models import (
    EXTRACTED_MODEL_CLASSES_BY_NAME,
    MERGED_MODEL_CLASSES_BY_NAME,
    RULE_MODEL_CLASSES_BY_NAME,
    AnyAdditiveModel,
    AnyExtractedModel,
    AnyMergedModel,
    AnyPreventiveModel,
    AnyRuleSetRequest,
    AnyRuleSetResponse,
    AnySubtractiveModel,
)
from mex.common.transform import ensure_prefix
from mex.common.types import Identifier

EXTRACTED_MODEL_ADAPTER: TypeAdapter[AnyExtractedModel] = TypeAdapter(
    Annotated[AnyExtractedModel, Field(discriminator="entityType")]
)


def _merge_extracted_items_and_apply_preventive_rule(
    merged_dict: dict[str, Any],
    mergeable_fields: list[str],
    extracted_items: list[AnyExtractedModel],
    rule: AnyPreventiveModel | None,
) -> None:
    """Merge a list of extracted items while applying a preventive rule.

    Collect unique values from the extracted items and write them into `merged_dict`,
    unless the primary source of the extracted item was blocked by the rule.

    Args:
        merged_dict: Mapping from field names to lists of merged values
        mergeable_fields: List of mergeable field names
        extracted_items: List of extracted items
        rule: Preventive rules with primary source identifiers, can be None
    """
    for extracted_item in extracted_items:
        for field_name in mergeable_fields:
            if rule is not None and extracted_item.hadPrimarySource in getattr(
                rule, field_name
            ):
                continue
            extracted_value = getattr(extracted_item, field_name)
            extend_list_in_dict(merged_dict, field_name, extracted_value)


def _apply_additive_rule(
    merged_dict: dict[str, Any],
    mergeable_fields: list[str],
    rule: AnyAdditiveModel,
) -> None:
    """Merge the values from an additive rule into a `merged_dict`.

    Args:
        merged_dict: Mapping from field names to lists of merged values
        mergeable_fields: List of mergeable field names
        rule: Additive rule with values to be added
    """
    for field_name in mergeable_fields:
        rule_value = getattr(rule, field_name)
        extend_list_in_dict(merged_dict, field_name, rule_value)


def _apply_subtractive_rule(
    merged_dict: dict[str, Any],
    mergeable_fields: list[str],
    rule: AnySubtractiveModel,
) -> None:
    """Prune values of a subtractive rule from a `merged_dict`.

    Args:
        merged_dict: Mapping from field names to lists of merged values
        mergeable_fields: List of mergeable field names
        rule: Subtractive rule with values to remove
    """
    for field_name in mergeable_fields:
        rule_value = getattr(rule, field_name)
        prune_list_in_dict(merged_dict, field_name, rule_value)


def create_merged_item(
    identifier: Identifier,
    extracted_items: list[AnyExtractedModel],
    rule_set: AnyRuleSetRequest | AnyRuleSetResponse | None,
) -> AnyMergedModel:
    """Merge a list of extracted items with a set of rules.

    Args:
        identifier: Identifier the finished merged item should have
        extracted_items: List of extracted items, can be empty
        rule_set: Rule set, with potentially empty rules

    Raises:
        InconsistentGraphError: When the graph response cannot be parsed

    Returns:
        Instance of a merged item
    """
    if rule_set:
        entity_type = ensure_prefix(rule_set.stemType, "Merged")
    elif extracted_items:
        entity_type = ensure_prefix(extracted_items[0].stemType, "Merged")
    else:
        msg = "One of rule_set or extracted_items is required."
        raise MExError(msg)
    fields = MERGEABLE_FIELDS_BY_CLASS_NAME[entity_type]
    cls = MERGED_MODEL_CLASSES_BY_NAME[entity_type]

    merged_dict: dict[str, Any] = {"identifier": identifier}

    _merge_extracted_items_and_apply_preventive_rule(
        merged_dict, fields, extracted_items, rule_set.preventive if rule_set else None
    )
    if rule_set:
        _apply_additive_rule(merged_dict, fields, rule_set.additive)
        _apply_subtractive_rule(merged_dict, fields, rule_set.subtractive)
    merged_item = reraising(
        ValidationError,
        InconsistentGraphError,
        cls.model_validate,
        merged_dict,
    )
    return cast(AnyMergedModel, merged_item)  # mypy, get a grip!


def merge_search_result_item(item: dict[str, Any]) -> AnyMergedModel:
    """Merge a single search result into a merged item.

    Args:
        item: Raw merged search result item from the graph response

    Raises:
        InconsistentGraphError: When the graph response item has inconsistencies

    Returns:
        AnyMergedModel instance
    """
    extracted_items = [
        EXTRACTED_MODEL_ADAPTER.validate_python(component)
        for component in item["components"]
        if component["entityType"] in EXTRACTED_MODEL_CLASSES_BY_NAME
    ]
    rules_raw = [
        component
        for component in item["components"]
        if component["entityType"] in RULE_MODEL_CLASSES_BY_NAME
    ]
    if len(rules_raw) == NUMBER_OF_RULE_TYPES:
        rule_set_response = transform_raw_rules_to_rule_set_response(rules_raw)
    elif len(rules_raw) == 0:
        rule_set_response = None
    else:
        msg = f"Unexpected number of rules found in graph: {len(rules_raw)}"
        raise InconsistentGraphError(msg)

    return create_merged_item(
        identifier=item["identifier"],
        extracted_items=extracted_items,
        rule_set=rule_set_response,
    )


def search_merged_items_in_graph(
    query_string: str | None = None,
    stable_target_id: str | None = None,
    entity_type: list[str] | None = None,
    skip: int = 0,
    limit: int = 100,
) -> MergedItemSearch:
    """Search for merged items.

    Args:
        query_string: Full text search query term
        stable_target_id: Optional stable target ID filter
        entity_type: Optional entity type filter
        skip: How many items to skip for pagination
        limit: How many items to return at most

    Raises:
        InconsistentGraphError: When the graph response has inconsistencies

    Returns:
        MergedItemSearch instance
    """
    graph = GraphConnector.get()
    result = graph.fetch_merged_items(
        query_string=query_string,
        stable_target_id=stable_target_id,
        entity_type=entity_type,
        skip=skip,
        limit=limit,
    )
    total: int = result["total"]
    items: list[AnyMergedModel] = [
        reraising(
            ValidationError,
            InconsistentGraphError,
            merge_search_result_item,
            item,
        )
        for item in result["items"]
    ]
    return reraising(
        ValidationError,
        InconsistentGraphError,
        MergedItemSearch,
        items=items,
        total=total,
    )
