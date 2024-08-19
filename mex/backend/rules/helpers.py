from collections.abc import Mapping
from typing import Any, Final

from mex.backend.graph.connector import GraphConnector
from mex.backend.graph.models import Result
from mex.common.exceptions import MExError
from mex.common.models import (
    ADDITIVE_MODEL_CLASSES_BY_NAME,
    PREVENTIVE_MODEL_CLASSES_BY_NAME,
    RULE_SET_RESPONSE_CLASSES_BY_NAME,
    SUBTRACTIVE_MODEL_CLASSES_BY_NAME,
    AnyRuleModel,
    AnyRuleSetRequest,
    AnyRuleSetResponse,
)
from mex.common.transform import ensure_postfix
from mex.common.types import Identifier

MODEL_CLASS_LOOKUP_BY_FIELD_NAME: Final[dict[str, Mapping[str, type[AnyRuleModel]]]] = {
    "additive": ADDITIVE_MODEL_CLASSES_BY_NAME,
    "subtractive": SUBTRACTIVE_MODEL_CLASSES_BY_NAME,
    "preventive": PREVENTIVE_MODEL_CLASSES_BY_NAME,
}


def _transform_graph_result_to_rule_set_response(
    graph_result: Result,
) -> AnyRuleSetResponse:
    """Transform the results of fetching a rule triple into a rule set response."""
    stem_types: list[str] = []
    stable_target_ids: list[str] = []
    response: dict[str, Any] = {}
    model: type[AnyRuleModel] | None
    rules = graph_result.one()["items"]

    if len(rules) != 3:
        raise MExError("inconsistent rule item count")

    for rule in rules:
        for field_name, model_class_lookup in MODEL_CLASS_LOOKUP_BY_FIELD_NAME.items():
            if model := model_class_lookup.get(rule.get("entityType")):
                response[field_name] = rule
                stem_types.append(model.stemType)
                stable_target_ids.extend(rule.pop("stableTargetId", []))

    if len(set(stem_types)) != 1:
        raise MExError("inconsistent rule item stem types")
    if len(set(stable_target_ids)) != 1:
        raise MExError("inconsistent rule item stableTargetIds")

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
    connector.create_rule_set(rule_set, stable_target_id)
    rule_types = [
        rule_set.additive.entityType,
        rule_set.subtractive.entityType,
        rule_set.preventive.entityType,
    ]
    graph_result = connector.fetch_rule_items(
        None,
        stable_target_id,
        rule_types,
        0,
        3,
    )
    return _transform_graph_result_to_rule_set_response(graph_result)


def get_rule_set_from_graph(
    stable_target_id: Identifier,
) -> AnyRuleSetResponse:
    """Read a rule set from the graph."""
    connector = GraphConnector.get()
    graph_result = connector.fetch_rule_items(
        None,
        stable_target_id,
        None,
        0,
        3,
    )
    return _transform_graph_result_to_rule_set_response(graph_result)


def update_and_get_rule_set(
    rule_set: AnyRuleSetRequest,
    stable_target_id: Identifier,
) -> AnyRuleSetResponse:
    """Merge a rule set into the graph and read it back."""
    connector = GraphConnector.get()
    if not connector.exists_merged_item(
        stable_target_id,
        [rule_set.stemType],
    ):
        raise MExError("no merged item found for given identifier and type")
    return create_and_get_rule_set(rule_set, stable_target_id)