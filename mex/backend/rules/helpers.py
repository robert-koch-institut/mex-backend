from collections.abc import Callable
from typing import Any

from mex.backend.fields import RULE_CLASS_LOOKUP_BY_FIELD_NAME
from mex.backend.graph.connector import GraphConnector
from mex.backend.graph.exceptions import InconsistentGraphError, NoResultFoundError
from mex.common.models import (
    RULE_SET_RESPONSE_CLASSES_BY_NAME,
    AnyAdditiveModel,
    AnyPreventiveModel,
    AnyRuleModel,
    AnyRuleSetRequest,
    AnyRuleSetResponse,
    AnySubtractiveModel,
)
from mex.common.transform import ensure_postfix, ensure_prefix
from mex.common.types import AnyPrimitiveType, Identifier, MergedPrimarySourceIdentifier


def transform_raw_rules_to_rule_set_response(
    raw_rules: list[dict[str, Any]],
) -> AnyRuleSetResponse:
    """Transform a set of plain rules into a rule set response."""
    stem_types: list[str] = []
    stable_target_ids: list[str] = []
    response: dict[str, Any] = {}
    model: type[AnyRuleModel] | None

    if (num_raw_rules := len(raw_rules)) != len(RULE_CLASS_LOOKUP_BY_FIELD_NAME):
        msg = f"inconsistent number of rules found: {num_raw_rules}"
        raise InconsistentGraphError(msg)

    for rule in raw_rules:
        for field_name, model_class_lookup in RULE_CLASS_LOOKUP_BY_FIELD_NAME.items():
            if model := model_class_lookup.get(str(rule.get("entityType"))):
                response[field_name] = rule
                stem_types.append(model.stemType)
                stable_target_ids.extend(rule.pop("stableTargetId", []))

    if len(set(stem_types)) != 1:
        msg = "inconsistent rule item stem types"
        raise InconsistentGraphError(msg)
    if len(set(stable_target_ids)) != 1:
        msg = f"inconsistent rule item stableTargetIds: {', '.join(stable_target_ids)}"
        raise InconsistentGraphError(msg)

    response["stableTargetId"] = stable_target_ids[0]
    response_class_name = ensure_postfix(stem_types[0], "RuleSetResponse")
    response_class = RULE_SET_RESPONSE_CLASSES_BY_NAME[response_class_name]
    return response_class.model_validate(response)


def create_and_get_rule_set(
    rule_set: AnyRuleSetRequest,
    stable_target_id: Identifier | None = None,
) -> AnyRuleSetResponse:
    """Merge a rule set into the graph and read it back."""
    if stable_target_id is None:
        stable_target_id = Identifier.generate()

    connector = GraphConnector.get()
    response_class_name = ensure_postfix(rule_set.stemType, "RuleSetResponse")
    response_class = RULE_SET_RESPONSE_CLASSES_BY_NAME[response_class_name]
    rule_set_response = response_class(
        additive=rule_set.additive,
        preventive=rule_set.preventive,
        subtractive=rule_set.subtractive,
        stableTargetId=stable_target_id,
    )
    connector.ingest([rule_set_response])
    rule_types = [
        rule_set.additive.entityType,
        rule_set.subtractive.entityType,
        rule_set.preventive.entityType,
    ]
    graph_result = connector.fetch_rule_items(
        query_string=None,
        identifier=None,
        stable_target_id=stable_target_id,
        entity_type=rule_types,
        referenced_identifiers=None,
        reference_field=None,
        skip=0,
        limit=3,
    )
    return transform_raw_rules_to_rule_set_response(graph_result.one()["items"])


def get_rule_set_from_graph(
    stable_target_id: Identifier,
) -> AnyRuleSetResponse | None:
    """Read a rule set from the graph."""
    connector = GraphConnector.get()
    graph_result = connector.fetch_rule_items(
        query_string=None,
        identifier=None,
        stable_target_id=stable_target_id,
        entity_type=None,
        referenced_identifiers=None,
        reference_field=None,
        skip=0,
        limit=3,
    )
    if raw_rules := graph_result.one()["items"]:
        return transform_raw_rules_to_rule_set_response(raw_rules)
    return None


def update_and_get_rule_set(
    rule_set: AnyRuleSetRequest,
    stable_target_id: Identifier,
) -> AnyRuleSetResponse:
    """Merge a rule set into the graph and read it back."""
    connector = GraphConnector.get()
    if not connector.exists_item(
        stable_target_id, ensure_prefix(rule_set.stemType, "Merged")
    ):
        msg = "no merged item found for given identifier and type"
        raise NoResultFoundError(msg)
    return create_and_get_rule_set(rule_set, stable_target_id)


def merge_rules(
    source_rule: AnyAdditiveModel | AnyPreventiveModel | AnySubtractiveModel,
    target_rule: AnyAdditiveModel | AnyPreventiveModel | AnySubtractiveModel,
    value_filter: Callable[[AnyPrimitiveType], bool] = bool,
) -> None:
    """Merge rules by combining filtered values from source to target.

    Args:
        source_rule: The rule to merge from
        target_rule: The rule to merge into
        value_filter: Function to filter which values to merge
    """
    for field in source_rule.model_fields:
        target_list = getattr(target_rule, field)
        for value in getattr(source_rule, field):
            if value_filter(value):
                target_list.append(value)


def merge_rule_sets(
    source_rule_set: AnyRuleSetResponse,
    target_rule_set: AnyRuleSetResponse,
    primary_source_identifiers: list[MergedPrimarySourceIdentifier],
) -> None:
    """Merge rule sets by combining rules from source to target.

    Args:
        source_rule_set: The rule set to merge from
        target_rule_set: The rule set to merge into
        primary_source_identifiers: List of primary source identifiers for filtering
    """
    merge_rules(source_rule_set.additive, target_rule_set.additive)
    merge_rules(source_rule_set.subtractive, target_rule_set.subtractive)
    merge_rules(
        source_rule_set.preventive,
        target_rule_set.preventive,
        lambda identifier: identifier in primary_source_identifiers,
    )


def remove_primary_source_from_rule(
    rule: AnyPreventiveModel,
    primary_source_identifier: MergedPrimarySourceIdentifier,
) -> None:
    """Remove a primary source identifier from all fields of a preventive rule.

    Args:
        rule: The preventive rule to modify
        primary_source_identifier: The identifier to remove from all field lists
    """
    for field in rule.model_fields:
        value_list = getattr(rule, field)
        while primary_source_identifier in value_list:
            value_list.remove(primary_source_identifier)
