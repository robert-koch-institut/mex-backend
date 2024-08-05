from typing import Annotated

from fastapi import APIRouter, HTTPException
from pydantic import PlainSerializer
from starlette import status

from mex.backend.graph.connector import GraphConnector
from mex.backend.serialization import to_primitive
from mex.common.models import AnyRuleModel
from mex.common.types import AnyMergedIdentifier, Identifier

router = APIRouter()


@router.post("/rule-item", tags=["editor"])
def create_rule(
    rule: AnyRuleModel,
) -> Annotated[AnyRuleModel, PlainSerializer(to_primitive)]:
    """Create a new rule."""
    connector = GraphConnector.get()
    stable_target_id = Identifier.generate()
    return connector.ingest_rule(stable_target_id, rule)


@router.put("/rule-item/{stableTargetId}", tags=["editor"])
def update_rule(
    stableTargetId: AnyMergedIdentifier, rule: AnyRuleModel
) -> Annotated[AnyRuleModel, PlainSerializer(to_primitive)]:
    """Update an existing rule."""
    connector = GraphConnector.get()
    if not connector.exists_merged_item(stableTargetId, [rule.stemType]):
        raise HTTPException(
            status.HTTP_412_PRECONDITION_FAILED,
            "Referenced merged item does not exist or is not of correct type.",
        )
    return connector.ingest_rule(stableTargetId, rule)
