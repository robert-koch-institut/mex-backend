from typing import Annotated

from fastapi import APIRouter
from pydantic import PlainSerializer

from mex.backend.graph.connector import GraphConnector
from mex.backend.serialization import to_primitive
from mex.common.models import AnyRuleModel

router = APIRouter()


@router.post("/rule-item", tags=["editor"])
def create_rule(
    rule: AnyRuleModel,
) -> Annotated[AnyRuleModel, PlainSerializer(to_primitive)]:
    """Create a new rule."""
    connector = GraphConnector.get()
    return connector.create_rule(rule)
