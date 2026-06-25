from typing import TYPE_CHECKING, Any

from mex.backend.graph.exceptions import InconsistentGraphError
from mex.common.models import (
    RULE_MODEL_CLASSES_BY_NAME,
    RULE_MODEL_CLASSES_BY_TYPE_BY_NAME,
    RULE_SET_RESPONSE_CLASSES_BY_NAME,
    AnyRuleModel,
    AnyRuleSetResponse,
)
from mex.common.transform import ensure_postfix

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Sequence


def transform_raw_rules_to_rule_set_response(
    raw_rules: Sequence[dict[str, Any]],
) -> AnyRuleSetResponse:
    """Transform a set of plain rules into a rule set response."""
    stem_types: list[str] = []
    stable_target_ids: list[str] = []
    response: dict[str, Any] = {}
    model: type[AnyRuleModel] | None

    if (num_raw_rules := len(raw_rules)) != len(RULE_MODEL_CLASSES_BY_TYPE_BY_NAME):
        msg = f"inconsistent number of rules found: {num_raw_rules}"
        raise InconsistentGraphError(msg)

    for rule in raw_rules:
        for (
            field_name,
            model_class_lookup,
        ) in RULE_MODEL_CLASSES_BY_TYPE_BY_NAME.items():
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


def transform_raw_rule_set_to_rule_set_response(
    raw: dict[str, Any],
) -> AnyRuleSetResponse:
    """Transform a rule-set-response shaped graph result into a rule set response.

    Args:
        raw: Mapping with one rule item per rule field (or `None` when absent),
            as produced by the `get_rule_set_response` query

    Returns:
        Validated rule set response
    """
    present_rules = {
        field: rule
        for field in RULE_MODEL_CLASSES_BY_TYPE_BY_NAME
        if (rule := raw.get(field)) is not None
    }
    if len(present_rules) != len(RULE_MODEL_CLASSES_BY_TYPE_BY_NAME):
        msg = f"inconsistent rule set, only found: {', '.join(sorted(present_rules))}"
        raise InconsistentGraphError(msg)

    stem_types = {
        RULE_MODEL_CLASSES_BY_NAME[str(rule["entityType"])].stemType
        for rule in present_rules.values()
    }
    if len(stem_types) != 1:
        msg = "inconsistent rule item stem types"
        raise InconsistentGraphError(msg)

    response_class_name = ensure_postfix(stem_types.pop(), "RuleSetResponse")
    response_class = RULE_SET_RESPONSE_CLASSES_BY_NAME[response_class_name]
    return response_class.model_validate(raw)
