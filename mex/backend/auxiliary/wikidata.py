from typing import Annotated

from fastapi import APIRouter, Query

from mex.backend.auxiliary.models import PagedResponseSchema
from mex.common.models import ExtractedOrganization, ExtractedPrimarySource
from mex.common.primary_source.extract import extract_seed_primary_sources
from mex.common.primary_source.transform import (
    get_primary_sources_by_name,
    transform_seed_primary_sources_to_extracted_primary_sources,
)
from mex.common.types import TextLanguage
from mex.common.wikidata.extract import search_organizations_by_label
from mex.common.wikidata.transform import (
    transform_wikidata_organizations_to_extracted_organizations,
)

router = APIRouter()


@router.get("/wikidata", status_code=200, tags=["wikidata"])
def search_organization_in_wikidata(
    q: str,
    offset: Annotated[int, Query(ge=0, le=10e10)] = 0,
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
    lang: TextLanguage = TextLanguage.EN,
) -> PagedResponseSchema[ExtractedOrganization]:
    """Search an organization in wikidata.

    Args:
        q: label of the organization to be searched
        offset: start page number
        limit: end page number
        lang: language of the label. Example: en, de

    Returns:
        Paginated list of ExtractedOrganization
    """
    organizations = search_organizations_by_label(q, offset, limit, lang)

    extracted_organizations = list(
        transform_wikidata_organizations_to_extracted_organizations(
            organizations, extracted_primary_source_wikidata()
        )
    )

    return PagedResponseSchema(
        total=len(extracted_organizations),
        offset=offset,
        limit=limit,
        results=[organization for organization in extracted_organizations],
    )


def extracted_primary_source_wikidata() -> ExtractedPrimarySource:
    """Load and return wikidata primary source."""
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
    return extracted_primary_source_wikidata
