from typing import Any

from mex.backend.rules.models import RuleSet


def transform_rule_set_request_to_rule_set(request: dict[str, Any]) -> RuleSet:
    """Transform a rule set request to a rule set model."""
    return RuleSet()
