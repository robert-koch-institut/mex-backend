from typing import TYPE_CHECKING, Annotated, Literal

from pydantic import Field

from mex.common.models import (
    AdditiveActivity,
    AdditiveVariable,
    BaseModel,
    SubtractiveActivity,
    SubtractiveVariable,
)
from mex.common.models.rule_set import BlockingRule

if TYPE_CHECKING:
    BlockingActivity = BlockingRule
    BlockingVariable = BlockingRule
else:
    from mex.common.models import BlockingActivity, BlockingVariable


class ActivityRuleSet(BaseModel):
    """Set of rules to be applied to one merged activity item."""

    entityType: Annotated[
        Literal["ActivityRuleSet"], Field(alias="$type", frozen=True)
    ] = "ActivityRuleSet"

    addValues: AdditiveActivity = AdditiveActivity()
    blockValues: SubtractiveActivity = SubtractiveActivity()
    blockPrimarySources: BlockingActivity = BlockingActivity()


class VariableRuleSet(BaseModel):
    """Set of rules to be applied to one merged variable item."""

    entityType: Annotated[Literal["RuleSet"], Field(alias="$type", frozen=True)] = (
        "RuleSet"
    )

    addValues: AdditiveVariable = AdditiveVariable()
    blockValues: SubtractiveVariable = SubtractiveVariable()
    blockPrimarySources: BlockingVariable = BlockingVariable()
