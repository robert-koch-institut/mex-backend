from typing import Any

from mex.backend.rules.models import ActivityRuleSet
from mex.common.models import AdditiveActivity, BlockingActivity, SubtractiveActivity


# deprecated?
def transform_rule_set_request_to_rule_set(request: dict[str, Any]) -> ActivityRuleSet:
    """Transform a rule set request to a rule set model."""
    add_values = AdditiveActivity()
    block_values = SubtractiveActivity()
    block_primary_sources = BlockingActivity()
    # XXX make this dynamic, and better
    for rule_type, rule_values in (
        ("addValues", add_values),
        ("blockValues", block_values),
        ("blockPrimarySources", block_primary_sources),
    ):
        for field_name, field_rule in request.items():
            setattr(rule_values, field_name, field_rule[rule_type])
    return ActivityRuleSet(
        addValues=add_values,
        blockValues=block_values,
        blockPrimarySources=block_primary_sources,
    )
