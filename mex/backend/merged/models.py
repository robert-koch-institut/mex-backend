from typing import Annotated

from pydantic import Field

from mex.common.models import AnyMergedModel, AnyPreviewModel, BaseModel


class MergedItemSearch(BaseModel):
    """Response body for the merged item search endpoint."""

    items: list[Annotated[AnyMergedModel, Field(discriminator="entityType")]]
    total: int


class PreviewItemSearch(BaseModel):
    """Response body for the preview item search endpoint."""

    items: list[Annotated[AnyPreviewModel, Field(discriminator="entityType")]]
    total: int
