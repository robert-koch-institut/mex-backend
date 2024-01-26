from pydantic import Field

from mex.common.models import AnyMergedModel, BaseModel


class MergedItemSearchResponse(BaseModel):
    """Response body for the merged item search endpoint."""

    total: int
    items: list[AnyMergedModel] = Field(discriminator="entityType")
