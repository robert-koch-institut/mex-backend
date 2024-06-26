from fastapi import APIRouter
from fastapi.responses import JSONResponse

from mex.backend.graph.connector import GraphConnector
from mex.backend.transform import to_primitive
from mex.common.models import AnyRuleModel

router = APIRouter()


@router.post("/rule-item", tags=["editor"])
def create_rule(rule: AnyRuleModel) -> AnyRuleModel:
    """Create a new rule."""
    connector = GraphConnector.get()
    return JSONResponse(  # type: ignore[return-value]
        to_primitive(connector.create_rule(rule)),
    )
