from typing import Annotated

from fastapi import APIRouter, Body, HTTPException, Path
from starlette import status

from mex.backend.graph.exceptions import NoResultFoundError
from mex.backend.rules.helpers import (
    create_and_get_rule_set,
    delete_rule_set_from_graph,
    get_rule_set_from_graph,
    update_and_get_rule_set,
)
from mex.common.models import AnyRuleSetRequest, AnyRuleSetResponse
from mex.common.types import Identifier

router = APIRouter()


@router.post("/rule-set", status_code=status.HTTP_201_CREATED, tags=["editor"])
def create_rule_set(
    rule_set: Annotated[AnyRuleSetRequest, Body(discriminator="entityType")],
) -> AnyRuleSetResponse:
    """Create a new rule set."""
    return create_and_get_rule_set(rule_set)


@router.get("/rule-set/{stableTargetId}", tags=["editor"])
def get_rule_set(
    stableTargetId: Annotated[Identifier, Path()],
) -> AnyRuleSetResponse:
    """Get a rule set."""
    if rule_set := get_rule_set_from_graph(stableTargetId):
        return rule_set
    raise HTTPException(status.HTTP_404_NOT_FOUND, "no rules found")


@router.put("/rule-set/{stableTargetId}", tags=["editor"])
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
)
def delete_rule_set(
    stableTargetId: Annotated[Identifier, Path()],
) -> None:
    """Delete a rule set by stableTargetId."""
    try:
        delete_rule_set_from_graph(stableTargetId)
    except NoResultFoundError as error:
        raise HTTPException(status.HTTP_404_NOT_FOUND) from error
