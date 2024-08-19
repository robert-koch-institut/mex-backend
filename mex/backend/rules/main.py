from fastapi import APIRouter
from starlette import status

from mex.backend.rules.helpers import (
    create_and_get_rule_set,
    get_rule_set_from_graph,
    update_and_get_rule_set,
)
from mex.common.models import AnyRuleSetRequest, AnyRuleSetResponse
from mex.common.types import Identifier

router = APIRouter()


@router.post("/rule-set", status_code=status.HTTP_201_CREATED, tags=["editor"])
def create_rule_set(rule_set: AnyRuleSetRequest) -> AnyRuleSetResponse:
    """Create a new rule set."""
    return create_and_get_rule_set(rule_set)


@router.get("/rule-set/{stableTargetId}", tags=["editor"])
def get_rule_set(stableTargetId: Identifier) -> AnyRuleSetResponse:
    """Get a rule set."""
    return get_rule_set_from_graph(stableTargetId)


@router.put("/rule-set/{stableTargetId}", tags=["editor"])
def update_rule_set(
    stableTargetId: Identifier,
    rule_set: AnyRuleSetRequest,
) -> AnyRuleSetResponse:
    """Update an existing rule set."""
    return update_and_get_rule_set(rule_set, stableTargetId)
