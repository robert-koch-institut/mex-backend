from collections import deque
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query

from mex.backend.auxiliary.constants import RKI_WIKIDATA_ID
from mex.backend.graph.connector import GraphConnector
from mex.backend.security import has_read_access
from mex.common.exceptions import EmptySearchResultError
from mex.common.models import ExtractedOrganization, PaginatedItemsContainer
from mex.common.wikidata.helpers import get_wikidata_organization_with_locations

router = APIRouter()


@router.get("/wikidata", tags=["auxiliary"], dependencies=[Depends(has_read_access)])
def search_organizations_in_wikidata(
    q: Annotated[str, Query(max_length=1000)] = RKI_WIKIDATA_ID,
) -> PaginatedItemsContainer[ExtractedOrganization]:
    """Search for organizations in wikidata.

    Args:
        q: Wikidata item ID or full concept URI

    Returns:
        Paginated list of ExtractedOrganizations
    """
    try:
        result = get_wikidata_organization_with_locations(q)
    except ValueError as error:
        raise HTTPException(400, list(error.args)) from None
    except EmptySearchResultError:
        raise HTTPException(404, q) from None

    if result is None:
        return PaginatedItemsContainer[ExtractedOrganization](
            items=[],
            total=0,
        )

    connector = GraphConnector.get()
    deque(connector.ingest_items([result.primary_source, *result.locations]))
    return PaginatedItemsContainer[ExtractedOrganization](
        items=[result.organization],
        total=1,
    )
