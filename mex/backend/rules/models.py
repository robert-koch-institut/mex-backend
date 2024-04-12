from typing import Annotated, Literal

from pydantic import Field

from mex.common.models import (
    AdditiveActivity,
    AdditiveVariable,
    BaseModel,
    PreventiveActivity,
    PreventiveVariable,
    SubtractiveActivity,
    SubtractiveVariable,
)


class ActivityRuleSet(BaseModel):
    """Set of rules to be applied to one merged activity item."""

    entityType: Annotated[
        Literal["ActivityRuleSet"], Field(alias="$type", frozen=True)
    ] = "ActivityRuleSet"

    addValues: AdditiveActivity = AdditiveActivity()
    blockValues: SubtractiveActivity = SubtractiveActivity()
    blockPrimarySources: PreventiveActivity = PreventiveActivity()


class VariableRuleSet(BaseModel):
    """Set of rules to be applied to one merged variable item."""

    entityType: Annotated[
        Literal["VariableRuleSet"], Field(alias="$type", frozen=True)
    ] = "VariableRuleSet"

    addValues: AdditiveVariable = AdditiveVariable()
    blockValues: SubtractiveVariable = SubtractiveVariable()
    blockPrimarySources: PreventiveVariable = PreventiveVariable()
