from mex.common.models import BaseModel
from mex.common.models.activity import OptionalActivity


class AddActivityValues(OptionalActivity):
    """Rule to add values to merged activity items."""


class BlockActivityValues(OptionalActivity):
    """Rule to block values from merged activity items."""


class BlockPrimarySource(BaseModel):
    """Rule to block a primary source on any merged item."""


class RuleSet(BaseModel):
    """Set of rules to be applied to one merged item."""

    add_values: AddActivityValues = AddActivityValues()
    block_values: BlockActivityValues = BlockActivityValues()
    block_primary_sources: list[BlockPrimarySource] = []
