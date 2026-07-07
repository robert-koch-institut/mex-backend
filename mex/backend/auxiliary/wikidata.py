from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query

from mex.backend.auxiliary.constants import RKI_WIKIDATA_ID, WIKIDATA_SOURCE_NAME
from mex.backend.auxiliary.helpers import cached_primary_source
from mex.backend.security import has_read_access
from mex.common.exceptions import EmptySearchResultError
from mex.common.models import ExtractedOrganization, PaginatedItemsContainer
from mex.common.wikidata.extract import get_wikidata_organization
from mex.common.wikidata.transform import (
    transform_wikidata_organizations_to_extracted_organizations,
)

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
        wikidata_organizations = [get_wikidata_organization(q)]
    except ValueError as error:
        raise HTTPException(400, list(error.args)) from None
    except EmptySearchResultError:
        raise HTTPException(404, q) from None
    extracted_organizations = list(
        transform_wikidata_organizations_to_extracted_organizations(
            wikidata_organizations,
            cached_primary_source(WIKIDATA_SOURCE_NAME).stableTargetId,
        )
    )
    return PaginatedItemsContainer[ExtractedOrganization](
        items=extracted_organizations,
        total=len(extracted_organizations),
    )
