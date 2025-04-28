from typing import Annotated

from fastapi import APIRouter, Query
from requests import HTTPError

from mex.backend.auxiliary.primary_source import extracted_primary_source_wikidata
from mex.common.models import ExtractedOrganization, PaginatedItemsContainer
from mex.common.wikidata.extract import get_wikidata_organization
from mex.common.wikidata.transform import (
    transform_wikidata_organizations_to_extracted_organizations,
)

router = APIRouter()


@router.get("/wikidata", tags=["editor"])
def search_organizations_in_wikidata(
    q: Annotated[str, Query(max_length=1000)] = "Q679041",
    offset: Annotated[int, Query(ge=0, le=10e10)] = 0,
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
) -> PaginatedItemsContainer[ExtractedOrganization]:
    """Search for organizations in wikidata.

    Args:
        q: Wikidata item ID or full URL
        offset: start page number
        limit: end page number

    Returns:
        Paginated list of ExtractedOrganizations
    """
    try:
        wikidata_organizations = [get_wikidata_organization(q)]
    except HTTPError:
        wikidata_organizations = []
    extracted_organizations = list(
        transform_wikidata_organizations_to_extracted_organizations(
            wikidata_organizations, extracted_primary_source_wikidata()
        )
    )
    return PaginatedItemsContainer[ExtractedOrganization](
        items=extracted_organizations[offset:limit], total=len(extracted_organizations)
    )
