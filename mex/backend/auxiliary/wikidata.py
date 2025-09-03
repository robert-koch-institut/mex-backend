from collections import deque
from typing import Annotated, cast

from fastapi import APIRouter, Query
from requests import HTTPError

from mex.backend.auxiliary.primary_source import extracted_primary_source_wikidata
from mex.backend.auxiliary.utils import fetch_extracted_item_by_source_identifiers
from mex.backend.graph.connector import GraphConnector
from mex.backend.graph.exceptions import NoResultFoundError
from mex.common.logging import logger
from mex.common.models import (
    MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
    ExtractedOrganization,
    PaginatedItemsContainer,
)
from mex.common.wikidata.extract import get_wikidata_organization
from mex.common.wikidata.transform import (
    transform_wikidata_organization_to_extracted_organization,
    transform_wikidata_organizations_to_extracted_organizations,
)

RKI_WIKIDATA_ID = "Q679041"

router = APIRouter()


@router.get("/wikidata", tags=["editor"])
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
    except HTTPError:
        wikidata_organizations = []
    extracted_organizations = list(
        transform_wikidata_organizations_to_extracted_organizations(
            wikidata_organizations, extracted_primary_source_wikidata()
        )
    )
    return PaginatedItemsContainer[ExtractedOrganization](
        items=extracted_organizations,
        total=len(extracted_organizations),
    )


def _fetch_or_insert_organization(name: str) -> ExtractedOrganization:
    """Fetch and return or load, ingest and return an organization by name."""
    extracted_item = fetch_extracted_item_by_source_identifiers(
        had_primary_source=MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
        identifier_in_primary_source=name,
    )
    if extracted_item:
        return cast("ExtractedOrganization", extracted_item)
    wikidata_organization = get_wikidata_organization(name)
    extracted_item = transform_wikidata_organization_to_extracted_organization(
        wikidata_organization,
        extracted_primary_source_wikidata(),
    )
    if not extracted_item:
        raise NoResultFoundError(name)
    connector = GraphConnector.get()
    deque(connector.ingest_extracted_items([extracted_item]))
    logger.info("ingested organization %s", name)
    return extracted_item


def extracted_organization_rki() -> ExtractedOrganization:
    """Get the rki organization."""
    return _fetch_or_insert_organization(RKI_WIKIDATA_ID)
