from typing import Any

from mex.backend.rules.models import AddActivityValues, RuleSet


def transform_rule_set_request_to_rule_set(request: dict[str, Any]) -> RuleSet:
    """Transform a rule set request to a rule set model."""
    add_values = AddActivityValues()
    for field_name, field_rule in request.items():
        setattr(add_values, field_name, field_rule["addValues"])
    return RuleSet(addValues=add_values)
