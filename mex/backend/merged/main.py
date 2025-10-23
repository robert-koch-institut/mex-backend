from collections.abc import Sequence
from typing import Annotated

from fastapi import APIRouter, Path, Query
from fastapi.exceptions import HTTPException
from starlette import status

from mex.backend.graph.connector import GraphConnector
from mex.backend.graph.exceptions import DeletionFailedError, NoResultFoundError
from mex.backend.merged.helpers import (
    delete_merged_item_from_graph,
    get_merged_item_from_graph,
    search_merged_items_in_graph,
)
from mex.backend.rules.helpers import get_rule_set_from_graph
from mex.backend.types import MergedType, ReferenceFieldName
from mex.common.models import (
    MERGED_MODEL_CLASSES_BY_NAME,
    AnyMergedModel,
    PaginatedItemsContainer,
)
from mex.common.types import Identifier, Validation

router = APIRouter()


@router.get("/merged-item", tags=["editor"])
def search_merged_items(  # noqa: PLR0913
    q: Annotated[str, Query(max_length=100)] = "",
    identifier: Annotated[Identifier | None, Query()] = None,
    entityType: Annotated[Sequence[MergedType], Query(max_length=len(MergedType))] = [],
    hadPrimarySource: Annotated[
        Sequence[Identifier] | None, Query(deprecated=True)
    ] = None,
    referencedIdentifier: Annotated[Sequence[Identifier] | None, Query()] = None,
    referenceField: Annotated[ReferenceFieldName | None, Query()] = None,
    skip: Annotated[int, Query(ge=0, le=10e10)] = 0,
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
) -> PaginatedItemsContainer[AnyMergedModel]:
    """Search for merged items by query text or by type and identifier."""
    if hadPrimarySource:
        referencedIdentifier = hadPrimarySource  # noqa: N806
        referenceField = ReferenceFieldName("hadPrimarySource")  # noqa: N806
    if bool(referencedIdentifier) != bool(referenceField):
        msg = "Must provide referencedIdentifier AND referenceField or neither."
        raise HTTPException(status.HTTP_400_BAD_REQUEST, msg)
    return search_merged_items_in_graph(
        query_string=q,
        identifier=identifier,
        entity_type=[str(t.value) for t in entityType or MergedType],
        referenced_identifiers=[str(s) for s in referencedIdentifier]
        if referencedIdentifier
        else None,
        reference_field=str(referenceField.value) if referenceField else None,
        skip=skip,
        limit=limit,
        validation=Validation.IGNORE,
    )


@router.get("/merged-item/{identifier}", tags=["editor"])
def get_merged_item(identifier: Annotated[Identifier, Path()]) -> AnyMergedModel:
    """Return one merged item for the given `identifier`."""
    try:
        return get_merged_item_from_graph(identifier)
    except NoResultFoundError as error:
        raise HTTPException(status.HTTP_404_NOT_FOUND) from error


@router.delete(
    "/merged-item/{identifier}", status_code=status.HTTP_204_NO_CONTENT, tags=["editor"]
)
def delete_merged_item(
    identifier: Annotated[Identifier, Path()],
    include_rule_set: Annotated[  # noqa: FBT002
        bool,
        Query(
            description="Delete with rule-set or "
            "fail if rule-set is present and this parameter is False."
        ),
    ] = False,
) -> None:
    """Delete one merged item for the given `identifier`."""
    connector = GraphConnector.get()
    if not connector.exists_item(identifier, list(MERGED_MODEL_CLASSES_BY_NAME)):
        raise HTTPException(status.HTTP_404_NOT_FOUND)
    if include_rule_set is False and get_rule_set_from_graph(identifier):
        raise HTTPException(status.HTTP_412_PRECONDITION_FAILED)
    try:
        delete_merged_item_from_graph(identifier)
    except DeletionFailedError as error:
        raise HTTPException(status.HTTP_409_CONFLICT) from error
