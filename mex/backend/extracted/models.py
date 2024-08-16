from typing import Annotated

from pydantic import Field

from mex.common.models import AnyExtractedModel, BaseModel


class ExtractedItemSearch(BaseModel):
    """Result of searching for extracted items in the graph."""

    items: list[Annotated[AnyExtractedModel, Field(discriminator="entityType")]]
    total: int
