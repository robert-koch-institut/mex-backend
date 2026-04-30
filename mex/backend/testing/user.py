from collections import deque
from typing import Annotated, cast

from fastapi import APIRouter, Depends

from mex.backend.graph.connector import GraphConnector
from mex.backend.merged.helpers import search_merged_items_in_graph
from mex.backend.testing.security import has_oidc_access_mocked
from mex.common.models import (
    MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
    ExtractedPerson,
    MergedPerson,
)
from mex.common.types import Validation

router = APIRouter()


@router.get("/user/me", tags=["oauth"])
def get_current_testing_user(
    username: Annotated[str, Depends(has_oidc_access_mocked)],
) -> MergedPerson:
    """Return a mocked MergedPerson for the testing app."""
    person = ExtractedPerson(
        hadPrimarySource=MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
        identifierInPrimarySource=username,
        fullName=[username],
        email=[f"{username}@rki.com"],
        orcidId=["https://orcid.org/1234-5678-9012-345"],
    )
    connector = GraphConnector.get()
    deque(connector.ingest_items([person]))

    result = search_merged_items_in_graph(
        query_string=username,
        identifier=None,
        entity_type=["MergedPerson"],
        reference_field=None,
        referenced_identifiers=None,
        skip=0,
        limit=1,
        validation=Validation.IGNORE,
    )
    return cast("MergedPerson", result.items[0])
