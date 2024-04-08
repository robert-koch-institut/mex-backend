from typing import Annotated

from pydantic import Field

from mex.common.models import AnyMergedModel, BaseModel


class MergedItemSearchResponse(BaseModel):
    """Response body for the merged item search endpoint."""

    total: int
    items: Annotated[list[AnyMergedModel], Field(discriminator="entityType")]
