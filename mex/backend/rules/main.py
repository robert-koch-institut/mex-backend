from collections.abc import Sequence
from typing import Annotated

from fastapi import APIRouter, Body, Depends, HTTPException, Path, Query
from starlette import status

from mex.backend.graph.exceptions import NoResultFoundError
from mex.backend.models import ReferenceFilter
from mex.backend.rules.helpers import (
    create_and_get_rule_set,
    delete_rule_set_from_graph,
    get_rule_set_from_graph,
    search_rule_items_in_graph,
    update_and_get_rule_set,
)
from mex.backend.security import has_read_access, has_write_access
from mex.backend.suggestion.constants import (
    DEFAULT_CANDIDATE_LIMIT,
    MAX_CANDIDATE_LIMIT,
)
from mex.backend.suggestion.helpers import suggest_vocabulary_values_from_graph
from mex.backend.suggestion.models import SuggestionResponse
from mex.backend.types import RuleType, VocabularyFieldName
from mex.common.models import (
    AnyAdditiveModel,
    AnyRuleModel,
    AnyRuleSetRequest,
    AnyRuleSetResponse,
    PaginatedItemsContainer,
)
from mex.common.types import Identifier

router = APIRouter()


@router.get("/rule-item", tags=["editor"], dependencies=[Depends(has_read_access)])
def search_rule_items(
    q: Annotated[str, Query(max_length=100)] = "",
    entityType: Annotated[Sequence[RuleType], Query(max_length=len(RuleType))] = [],
    skip: Annotated[int, Query(ge=0, le=10e10)] = 0,
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
) -> PaginatedItemsContainer[AnyRuleModel]:
    """Search for rule items using simple filters.

    For complex queries combining multiple reference filters, use POST
    /rule-item/_search
    """
    return search_rule_items_in_graph(
        query_string=q,
        entity_type=[str(t.value) for t in entityType or RuleType],
        skip=skip,
        limit=limit,
    )


@router.post(
    "/rule-item/_search", tags=["editor"], dependencies=[Depends(has_read_access)]
)
def search_rule_items_advanced(
    q: Annotated[str, Body(max_length=100)] = "",
    entityType: Annotated[Sequence[RuleType], Body(max_length=len(RuleType))] = [],
    referenceFilters: Annotated[Sequence[ReferenceFilter], Body(max_length=100)]
    | None = None,
    skip: Annotated[int, Body(ge=0, le=10e10)] = 0,
    limit: Annotated[int, Body(ge=1, le=100)] = 10,
) -> PaginatedItemsContainer[AnyRuleModel]:
    """Search for rule items with advanced filter combinations.

    Use this endpoint for:
    - Multiple reference filters combined with AND logic, e.g. hadPrimarySource AND
      unitOf
    """
    return search_rule_items_in_graph(
        query_string=q,
        entity_type=[str(t.value) for t in entityType or RuleType],
        reference_filters=referenceFilters,
        skip=skip,
        limit=limit,
    )


@router.post(
    "/rule-item/_suggest", tags=["editor"], dependencies=[Depends(has_read_access)]
)
def suggest_rule_item_values(
    additive: Annotated[AnyAdditiveModel, Body(discriminator="entityType")],
    fields: Annotated[
        Sequence[VocabularyFieldName], Body(max_length=len(VocabularyFieldName))
    ] = [],
    excludeIdentifier: Annotated[Identifier | None, Body()] = None,
    limit: Annotated[int, Body(ge=1, le=MAX_CANDIDATE_LIMIT)] = DEFAULT_CANDIDATE_LIMIT,
    minConfidence: Annotated[float, Body(ge=0.0, le=1.0)] = 0.0,
) -> SuggestionResponse:
    """Suggest likely vocabulary values for a partially filled draft item.

    Intended for data entry forms: post the item as far as the user has filled
    it in and get back, per vocabulary field, the values that comparable
    existing items carry. What to do with them, for example prefilling a field
    or merely reordering a dropdown, is left to the client.

    Evidence is drawn from three graph derived signals, blended and shrunk
    toward the corpus wide prior:

    - the references already on the draft, e.g. items of the same `unitInCharge`
    - the draft's free text, matched against the full text index, where the top
      scoring items vote weighted by their relevance
    - the unconditioned distribution over all items of the same entity type

    By default only the fields the draft has left empty are ranked. Pass
    `excludeIdentifier` when re-editing an existing item, so that it cannot vote
    for itself.

    The counts behind these suggestions honour additive and subtractive rules,
    but ignore preventive rules and primary source precedence. They describe
    what comparable records look like, never what is correct for this record, so
    do not reuse them where a fact is required.

    This endpoint is meant to be called while the user types. Debounce by at
    least 300ms and cancel requests that are still in flight when the draft
    changes again.
    """
    return suggest_vocabulary_values_from_graph(
        additive=additive,
        requested_fields=fields,
        exclude_identifier=str(excludeIdentifier) if excludeIdentifier else None,
        limit=limit,
        min_confidence=minConfidence,
    )


@router.post(
    "/rule-set",
    status_code=status.HTTP_201_CREATED,
    tags=["editor"],
    dependencies=[Depends(has_write_access)],
)
def create_rule_set(
    rule_set: Annotated[AnyRuleSetRequest, Body(discriminator="entityType")],
) -> AnyRuleSetResponse:
    """Create a new rule set."""
    return create_and_get_rule_set(rule_set)


@router.get(
    "/rule-set/{stableTargetId}",
    tags=["editor"],
    dependencies=[Depends(has_read_access)],
)
def get_rule_set(
    stableTargetId: Annotated[Identifier, Path()],
) -> AnyRuleSetResponse:
    """Get a rule set."""
    if rule_set := get_rule_set_from_graph(stableTargetId):
        return rule_set
    raise HTTPException(status.HTTP_404_NOT_FOUND, "no rules found")


@router.put(
    "/rule-set/{stableTargetId}",
    tags=["editor"],
    dependencies=[Depends(has_write_access)],
)
def update_rule_set(
    stableTargetId: Annotated[Identifier, Path()],
    rule_set: Annotated[AnyRuleSetRequest, Body(discriminator="entityType")],
) -> AnyRuleSetResponse:
    """Update an existing rule set."""
    return update_and_get_rule_set(rule_set, stableTargetId)


@router.delete(
    "/rule-set/{stableTargetId}",
    status_code=status.HTTP_204_NO_CONTENT,
    tags=["editor"],
    dependencies=[Depends(has_write_access)],
)
def delete_rule_set(
    stableTargetId: Annotated[Identifier, Path()],
) -> None:
    """Delete a rule set by stableTargetId."""
    try:
        delete_rule_set_from_graph(stableTargetId)
    except NoResultFoundError as error:
        raise HTTPException(status.HTTP_404_NOT_FOUND) from error
