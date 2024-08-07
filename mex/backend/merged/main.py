from collections.abc import Sequence
from typing import Annotated

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
from pydantic import Field, TypeAdapter, ValidationError

from mex.backend.graph.connector import GraphConnector
from mex.backend.merged.models import MergedItemSearchResponse
from mex.backend.transform import to_primitive
from mex.backend.types import MergedType, UnprefixedType
from mex.common.logging import logger
from mex.common.models import AnyMergedModel
from mex.common.transform import ensure_prefix
from mex.common.types import Identifier

router = APIRouter()


@router.get("/merged-item", tags=["editor", "public"])
def search_merged_items_facade(
    q: Annotated[str, Query(max_length=100)] = "",
    stableTargetId: Annotated[Identifier | None, Query(deprecated=True)] = None,
    identifier: Identifier | None = None,
    entityType: Annotated[
        Sequence[MergedType | UnprefixedType], Query(max_length=len(MergedType))
    ] = [],
    skip: Annotated[int, Query(ge=0, le=10e10)] = 0,
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
) -> MergedItemSearchResponse:
    """Facade for retrieving merged items."""
    # XXX We just search for extracted items and pretend they are already merged
    #     as a stopgap for MX-1382.
    graph = GraphConnector.get()
    result = graph.fetch_extracted_data(
        q,
        identifier or stableTargetId,
        [
            # Allow 'MergedPerson' as well as 'Person' as an entityType for this
            # endpoint to keep compatibility with previous API clients.
            ensure_prefix(t.value.removeprefix("Merged"), "Extracted")
            for t in entityType or MergedType
        ],
        skip,
        limit,
    )
    merged_model_adapter: TypeAdapter[AnyMergedModel] = TypeAdapter(
        Annotated[AnyMergedModel, Field(discriminator="entityType")]
    )
    items: list[AnyMergedModel] = []
    total: int = result["total"]

    for item in result["items"]:
        item.pop("hadPrimarySource", None)
        item.pop("identifierInPrimarySource", None)
        item["identifier"] = item.pop("stableTargetId")
        item["entityType"] = item["entityType"].replace("Extracted", "Merged")
        try:
            items.append(merged_model_adapter.validate_python(item))
        except ValidationError as error:
            logger.error(error)

    response = MergedItemSearchResponse(items=items, total=total)
    return JSONResponse(to_primitive(response))  # type: ignore[return-value]
