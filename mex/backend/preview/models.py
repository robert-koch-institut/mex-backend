from typing import Annotated, Self

from pydantic import Field, model_validator

from mex.common.models import (
    AnyAdditiveModel,
    AnyPreventiveModel,
    AnySubtractiveModel,
    BaseModel,
)


class RuleSet(BaseModel):
    """A set of three rules for one distinct stem type."""

    additive: Annotated[AnyAdditiveModel, Field(discriminator="entityType")]
    preventive: Annotated[AnyPreventiveModel, Field(discriminator="entityType")]
    subtractive: Annotated[AnySubtractiveModel, Field(discriminator="entityType")]

    @model_validator(mode="after")
    def validate_stem_type(self) -> Self:
        """Validate that all rules have the same stem type."""
        if not (
            self.additive.stemType
            == self.preventive.stemType
            == self.subtractive.stemType
        ):
            raise AssertionError("All rules must have same stemType")
        return self

    @property
    def stemType(self) -> str:  # noqa: N802
        """Return the stem type of the contained rules."""
        return self.additive.stemType
