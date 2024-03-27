from typing import Annotated, Any

from fastapi import APIRouter, Body

from mex.backend.graph.connector import GraphConnector
from mex.backend.rules.models import ActivityRuleSet
from mex.backend.rules.transform import transform_rule_set_request_to_rule_set

router = APIRouter()


@router.post("/rule-set", status_code=204, tags=["editor"])
def create_rule_set(
    # request: Annotated[dict[str, Any], Body()],
    rule_set: ActivityRuleSet
) -> None:
    """Create a new set of rules."""
    # rule_set = transform_rule_set_request_to_rule_set(request)
    connector = GraphConnector.get()
    connector.create_rule_set(rule_set)
