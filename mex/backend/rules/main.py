from typing import Annotated

from fastapi import APIRouter
from pydantic import PlainSerializer
from starlette import status

from mex.backend.rules.helpers import create_and_get_rule_set, get_rule_set_from_graph
from mex.backend.serialization import to_primitive
from mex.common.models import AnyRuleSetRequest, AnyRuleSetResponse
from mex.common.types import Identifier

router = APIRouter()


@router.post("/rule-set", status_code=status.HTTP_201_CREATED, tags=["editor"])
def create_rule_set(
    rule_set: AnyRuleSetRequest,
) -> Annotated[AnyRuleSetResponse, PlainSerializer(to_primitive)]:
    """Create a new rule set."""
    return create_and_get_rule_set(rule_set)


@router.get("/rule-set/{stableTargetId}", tags=["editor"])
def get_rule_set(
    stableTargetId: Identifier,
) -> Annotated[AnyRuleSetResponse, PlainSerializer(to_primitive)]:
    """Get a rule set."""
    return get_rule_set_from_graph(stableTargetId)
