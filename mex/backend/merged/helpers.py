from typing import Annotated, Any

from pydantic import Field, TypeAdapter, ValidationError

from mex.backend.fields import MERGEABLE_FIELDS_BY_CLASS_NAME
from mex.backend.graph.connector import GraphConnector
from mex.backend.merged.models import MergedItemSearch
from mex.backend.utils import extend_list_in_dict, prune_list_in_dict
from mex.common.logging import logger
from mex.common.models import (
    MERGED_MODEL_CLASSES_BY_NAME,
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


def _apply_preventive_rule(
    merged_dict: dict[str, Any],
    mergeable_fields: list[str],
    extracted_items: list[AnyExtractedModel],
    rule: AnyPreventiveModel,
) -> None:
    """Merge a list of extracted items while applying a preventive rule.

    Collect unique values from the extracted items and write them into `merged_dict`,
    unless the primary source of the extracted item was blocked by the rule.

    Args:
        merged_dict: Mapping from field names to lists of merged values
        mergeable_fields: List of mergeable field names
        extracted_items: List of extracted items
        rule: Preventive rules with primary source identifiers
    """
    for extracted_item in extracted_items:
        for field_name in mergeable_fields:
            if extracted_item.hadPrimarySource not in getattr(rule, field_name):
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
    rule_set: AnyRuleSetRequest | AnyRuleSetResponse,
) -> AnyMergedModel:
    """Merge a list of extracted items with a set of rules.

    Args:
        identifier: Identifier the finished merged item should have
        extracted_items: List of extracted items, can be empty
        rule_set: Rule set, with potentially empty rules

    Returns:
        Instance of a merged item
    """
    entity_type = ensure_prefix(rule_set.stemType, "Merged")
    fields = MERGEABLE_FIELDS_BY_CLASS_NAME[entity_type]
    cls = MERGED_MODEL_CLASSES_BY_NAME[entity_type]

    merged_dict: dict[str, Any] = {"identifier": identifier}
    _apply_preventive_rule(merged_dict, fields, extracted_items, rule_set.preventive)
    _apply_additive_rule(merged_dict, fields, rule_set.additive)
    _apply_subtractive_rule(merged_dict, fields, rule_set.subtractive)

    return cls.model_validate(merged_dict)


def search_merged_items_in_graph(
    query_string: str | None = None,
    stable_target_id: str | None = None,
    entity_type: list[str] | None = None,
    skip: int = 0,
    limit: int = 100,
) -> MergedItemSearch:
    """Facade for searching for merged items.

    Args:
        query_string: Full text search query term
        stable_target_id: Optional stable target ID filter
        entity_type: Optional entity type filter
        skip: How many items to skip for pagination
        limit: How many items to return at most

    Returns:
        MergedItemSearch instance
    """
    # XXX We just search for extracted items and pretend they are already merged
    #     as a stopgap for MX-1382.
    graph = GraphConnector.get()
    result = graph.fetch_extracted_items(
        query_string=query_string,
        stable_target_id=stable_target_id,
        entity_type=(
            None
            if entity_type is None
            else [t.replace("Merged", "Extracted") for t in entity_type]
        ),
        skip=skip,
        limit=limit,
    )
    merged_model_adapter: TypeAdapter[AnyMergedModel] = TypeAdapter(
        Annotated[AnyMergedModel, Field(discriminator="entityType")]
    )
    items: list[AnyMergedModel] = []
    total: int = result["total"]

    for item in result["items"]:
        item.pop("hadPrimarySource", None)
        item.pop("identifierInPrimarySource", None)
        item["identifier"] = item.pop("stableTargetId")
        item["entityType"] = item["entityType"].replace("Extracted", "Merged")
        try:
            items.append(merged_model_adapter.validate_python(item))
        except ValidationError as error:
            logger.error(error)

    return MergedItemSearch(items=items, total=total)
