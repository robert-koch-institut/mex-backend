from fastapi import APIRouter, HTTPException

from mex.backend.graph.connector import GraphConnector
from mex.common.models import AnyRuleModel
from mex.common.types import AnyMergedIdentifier, Identifier

router = APIRouter()


@router.post("/rule-item", tags=["editor"])
def create_rule(rule: AnyRuleModel) -> AnyRuleModel:
    """Create a new rule."""
    connector = GraphConnector.get()
    stable_target_id = Identifier.generate()
    return connector.ingest_rule(stable_target_id, rule)


@router.put("/rule-item/{stableTargetId}", tags=["editor"])
def update_rule(
    stableTargetId: AnyMergedIdentifier, rule: AnyRuleModel
) -> AnyRuleModel:
    """Update an existing rule."""
    connector = GraphConnector.get()
    if not connector.exists_merged_item(rule.stemType, stableTargetId):
        raise HTTPException(
            412, "Referenced merged item does not exist or is not of correct type."
        )
    return connector.ingest_rule(stableTargetId, rule)
