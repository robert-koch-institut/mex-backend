from typing import Annotated

from fastapi import APIRouter, Query

from mex.backend.auxiliary.primary_source import extracted_primary_source_wikidata
from mex.common.models import ExtractedOrganization, PaginatedItemsContainer
from mex.common.types import TextLanguage
from mex.common.wikidata.extract import (
    get_count_of_found_organizations_by_label,
    search_organizations_by_label,
)
from mex.common.wikidata.transform import (
    transform_wikidata_organizations_to_extracted_organizations,
)

router = APIRouter()


@router.get("/wikidata", tags=["editor"])
def search_organization_in_wikidata(
    q: Annotated[str, Query(min_length=1, max_length=1000)],
    offset: Annotated[int, Query(ge=0, le=10e10)] = 0,
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
) -> PaginatedItemsContainer[ExtractedOrganization]:
    """Search for organizations in wikidata.

    Args:
        q: label of the organization to be searched
        offset: start page number
        limit: end page number

    Returns:
        Paginated list of ExtractedOrganizations
    """
    total_count = get_count_of_found_organizations_by_label(q, TextLanguage.EN)
    wikidata_organizations = search_organizations_by_label(
        q, offset, limit, TextLanguage.EN
    )
    extracted_organizations = list(
        transform_wikidata_organizations_to_extracted_organizations(
            wikidata_organizations, extracted_primary_source_wikidata()
        )
    )
    return PaginatedItemsContainer[ExtractedOrganization](
        items=extracted_organizations, total=total_count
    )
