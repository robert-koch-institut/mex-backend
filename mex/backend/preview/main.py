from typing import Any, Annotated
from pydantic import Field

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from mex.backend.graph.connector import GraphConnector
from mex.backend.transform import to_primitive
from mex.common.models import AnyAdditiveModel
from mex.common.types import Identifier

router = APIRouter()


@router.post("/preview-item/{stableTargetId}", tags=["editor"])
def preview_item(
    stableTargetId: Identifier,
    request: Annotated[AnyAdditiveModel, Field(discriminator="entityType")],
) -> dict[str, Any]:
    connector = GraphConnector.get()
    extracted_items = connector.fetch_extracted_data(
        query_string=None,
        stable_target_id=stableTargetId,
        entity_type=None,
        skip=0,
        limit=100,
    ).one().get("items")
    result = {}
    rule = request.model_dump()
    for item in extracted_items:
        for field, value in item.items():
            result.setdefault(field, [])
            if isinstance(value,list):
                result[field].extend(value)
            else:
                result[field].append(value)
    for field,value in rule.items():
        result.setdefault(field, [])
        if isinstance(value,list):
            result[field].extend(value)
        else:
            result[field].append(value)

    return JSONResponse(  # type: ignore[return-value]
        to_primitive(result)
    )
