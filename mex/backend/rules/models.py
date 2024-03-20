from mex.common.models import BaseModel
from mex.common.models.activity import OptionalActivity
from mex.common.types import MergedPrimarySourceIdentifier


class AddActivityValues(OptionalActivity):
    """Rule to add values to merged activity items."""


class BlockActivityValues(OptionalActivity):
    """Rule to block values from merged activity items."""


class BlockActivityPrimarySources(BaseModel):
    """Rule to block a primary source on any merged item."""
    abstract: list[MergedPrimarySourceIdentifier] = []
    activityType: list[MergedPrimarySourceIdentifier] = []
    alternativeTitle: list[MergedPrimarySourceIdentifier] = []
    contact: list[MergedPrimarySourceIdentifier] = []
    documentation: list[MergedPrimarySourceIdentifier] = []
    end: list[MergedPrimarySourceIdentifier] = []
    externalAssociate: list[MergedPrimarySourceIdentifier] = []
    funderOrCommissioner: list[MergedPrimarySourceIdentifier] = []
    fundingProgram: list[MergedPrimarySourceIdentifier] = []
    involvedPerson: list[MergedPrimarySourceIdentifier] = []
    involvedUnit: list[MergedPrimarySourceIdentifier] = []
    isPartOfActivity: list[MergedPrimarySourceIdentifier] = []
    publication: list[MergedPrimarySourceIdentifier] = []
    responsibleUnit: list[MergedPrimarySourceIdentifier] = []
    shortName: list[MergedPrimarySourceIdentifier] = []
    start: list[MergedPrimarySourceIdentifier] = []
    succeeds: list[MergedPrimarySourceIdentifier] = []
    theme: list[MergedPrimarySourceIdentifier] = []
    title: list[MergedPrimarySourceIdentifier] = []
    website: list[MergedPrimarySourceIdentifier] = []

class RuleSet(BaseModel):
    """Set of rules to be applied to one merged item."""

    addValues: AddActivityValues = AddActivityValues()
    blockValues: BlockActivityValues = BlockActivityValues()
    blockPrimarySources: BlockActivityPrimarySources= BlockActivityPrimarySources()
