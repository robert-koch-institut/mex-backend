from typing import Any

from mex.backend.fields import RULE_CLASS_LOOKUP_BY_FIELD_NAME
from mex.backend.graph.exceptions import InconsistentGraphError
from mex.common.models import (
    RULE_SET_RESPONSE_CLASSES_BY_NAME,
    AnyRuleModel,
    AnyRuleSetResponse,
)
from mex.common.transform import ensure_postfix


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
