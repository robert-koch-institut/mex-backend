from typing import Annotated

from pydantic import Field

from mex.common.models import AnyMergedModel, BaseModel


class MergedItemSearch(BaseModel):
    """Response body for the merged item search endpoint."""

    items: list[Annotated[AnyMergedModel, Field(discriminator="entityType")]]
    total: int
