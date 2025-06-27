from collections.abc import Mapping
from typing import Any, Final

from mex.backend.graph.connector import GraphConnector
from mex.backend.graph.exceptions import InconsistentGraphError, NoResultFoundError
from mex.common.models import (
    ADDITIVE_MODEL_CLASSES_BY_NAME,
    PREVENTIVE_MODEL_CLASSES_BY_NAME,
    RULE_SET_RESPONSE_CLASSES_BY_NAME,
    SUBTRACTIVE_MODEL_CLASSES_BY_NAME,
    AnyRuleModel,
    AnyRuleSetRequest,
    AnyRuleSetResponse,
)
from mex.common.transform import ensure_postfix, ensure_prefix
from mex.common.types import Identifier

MODEL_CLASS_LOOKUP_BY_FIELD_NAME: Final[dict[str, Mapping[str, type[AnyRuleModel]]]] = {
    "additive": ADDITIVE_MODEL_CLASSES_BY_NAME,
    "subtractive": SUBTRACTIVE_MODEL_CLASSES_BY_NAME,
    "preventive": PREVENTIVE_MODEL_CLASSES_BY_NAME,
}


def transform_raw_rules_to_rule_set_response(
    raw_rules: list[dict[str, Any]],
) -> AnyRuleSetResponse:
    """Transform a set of plain rules into a rule set response."""
    stem_types: list[str] = []
    stable_target_ids: list[str] = []
    response: dict[str, Any] = {}
    model: type[AnyRuleModel] | None

    if (num_raw_rules := len(raw_rules)) != len(MODEL_CLASS_LOOKUP_BY_FIELD_NAME):
        msg = f"inconsistent number of rules found: {num_raw_rules}"
        raise InconsistentGraphError(msg)

    for rule in raw_rules:
        for field_name, model_class_lookup in MODEL_CLASS_LOOKUP_BY_FIELD_NAME.items():
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
