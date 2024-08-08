from typing import Annotated

from pydantic import Field, TypeAdapter, ValidationError

from mex.backend.graph.connector import GraphConnector
from mex.backend.merged.models import MergedItemSearch
from mex.common.logging import logger
from mex.common.models import AnyMergedModel


def search_merged_items_in_graph(
    query_string: str | None = None,
    stable_target_id: str | None = None,
    entity_type: list[str] | None = None,
    skip: int = 0,
    limit: int = 100,
) -> MergedItemSearch:
    """Facade for searching for merged items.

    Args:
        query_string: Full text search query term
        stable_target_id: Optional stable target ID filter
        entity_type: Optional entity type filter
        skip: How many items to skip for pagination
        limit: How many items to return at most

    Returns:
        MergedItemSearch instance
    """
    # XXX We just search for extracted items and pretend they are already merged
    #     as a stopgap for MX-1382.
    graph = GraphConnector.get()
    result = graph.fetch_extracted_items(
        query_string=query_string,
        stable_target_id=stable_target_id,
        entity_type=(
            None
            if entity_type is None
            else [t.replace("Merged", "Extracted") for t in entity_type]
        ),
        skip=skip,
        limit=limit,
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

    return MergedItemSearch(items=items, total=total)
