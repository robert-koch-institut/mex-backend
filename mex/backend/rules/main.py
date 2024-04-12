from fastapi import APIRouter

from mex.backend.graph.connector import GraphConnector
from mex.backend.rules.models import ActivityRuleSet

router = APIRouter()


@router.post("/rule-set", status_code=204, tags=["editor"])
def create_rule_set(
    rule_set: ActivityRuleSet,
) -> None:
    """Create a new set of rules."""
    connector = GraphConnector.get()
    connector.create_rule_set(rule_set)
