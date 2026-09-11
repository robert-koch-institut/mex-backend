from enum import Enum
from typing import Annotated

from pydantic import BaseModel, Field

from mex.backend.types import MergedType, VocabularyFieldName


class SuggestionSource(Enum):
    """Which evidence set dominated the suggestions for a field."""

    COHORT = "cohort"
    NEIGHBOURHOOD = "neighbourhood"
    CORPUS = "corpus"


class VocabularyCandidate(BaseModel):
    """One candidate value for a vocabulary field, with its evidence."""

    value: str
    confidence: Annotated[float, Field(ge=0.0, le=1.0)]
    support: Annotated[
        int,
        Field(
            ge=0,
            description=(
                "Lower bound on the number of distinct existing items backing this "
                "value. The cohort and the neighbourhood overlap and the queries do "
                "not return item identifiers, so this is the larger of the two counts "
                "rather than the size of their union, and can never overstate."
            ),
        ),
    ]


class FieldSuggestion(BaseModel):
    """Ranked candidate values for a single vocabulary field."""

    field: VocabularyFieldName
    candidates: list[VocabularyCandidate]
    sampleSize: Annotated[
        int,
        Field(
            ge=0,
            description=(
                "Number of items that carry any value in this field, i.e. the "
                "denominator behind the confidences. Items that left the field empty "
                "are excluded, so a sparsely filled field is not penalised."
            ),
        ),
    ]
    source: SuggestionSource


class SuggestionResponse(BaseModel):
    """Per field vocabulary suggestions derived from a partial draft item."""

    entityType: MergedType
    cohortSize: Annotated[int, Field(ge=0)]
    neighbourhoodSize: Annotated[int, Field(ge=0)]
    suggestions: list[FieldSuggestion]
