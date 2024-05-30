from fastapi import APIRouter

from mex.backend.graph.connector import GraphConnector
from mex.backend.types import AnyRule

router = APIRouter()


@router.post("/rule", tags=["editor"])
def create_rule(rule: AnyRule) -> AnyRule:
    """Create a new rule."""
    connector = GraphConnector.get()
    return connector.create_rule(rule)
