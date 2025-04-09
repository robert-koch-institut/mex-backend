from functools import cache
from typing import Annotated

from fastapi import APIRouter, Query

from mex.backend.graph.connector import GraphConnector
from mex.common.models import (
    ExtractedOrganization,
    ExtractedPrimarySource,
    PaginatedItemsContainer,
)
from mex.common.primary_source.extract import extract_seed_primary_sources
from mex.common.primary_source.transform import (
    get_primary_sources_by_name,
    transform_seed_primary_sources_to_extracted_primary_sources,
)
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


@cache
def extracted_primary_source_wikidata() -> ExtractedPrimarySource:
    """Load, ingest and return wikidata primary source."""
    seed_primary_sources = extract_seed_primary_sources()
    extracted_primary_sources = list(
        transform_seed_primary_sources_to_extracted_primary_sources(
            seed_primary_sources
        )
    )
    (extracted_primary_source_wikidata,) = get_primary_sources_by_name(
        extracted_primary_sources,
        "wikidata",
    )
    connector = GraphConnector.get()
    connector.ingest([extracted_primary_source_wikidata])
    return extracted_primary_source_wikidata
