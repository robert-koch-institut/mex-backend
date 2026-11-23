from collections import deque
from functools import lru_cache
from typing import TYPE_CHECKING, Protocol

from mex.backend.auxiliary.constants import (
    ORGANIGRAM_PRIMARY_SOURCE_NAME,
    RKI_WIKIDATA_ID,
    WIKIDATA_SOURCE_NAME,
)
from mex.backend.graph.connector import GraphConnector
from mex.backend.graph.exceptions import NoResultFoundError
from mex.common.logging import logger
from mex.common.models import PaginatedItemsContainer
from mex.common.organigram.extract import extract_organigram_units
from mex.common.organigram.transform import (
    transform_organigram_units_to_organizational_units,
)
from mex.common.primary_source.helpers import get_extracted_primary_source_by_name
from mex.common.wikidata.extract import get_wikidata_organization
from mex.common.wikidata.transform import (
    transform_wikidata_organization_to_extracted_organization,
)

if TYPE_CHECKING:
    from mex.common.models import (
        ExtractedOrganization,
        ExtractedOrganizationalUnit,
        ExtractedPrimarySource,
    )


@lru_cache
def cached_primary_source(name: str) -> ExtractedPrimarySource:
    """Ingest and return a primary source by name."""
    extracted_item = get_extracted_primary_source_by_name(name)
    if not extracted_item:
        raise NoResultFoundError(name)
    connector = GraphConnector.get()
    deque(connector.ingest_items([extracted_item]))
    logger.info("ingested primary source %s", name)
    return extracted_item


@lru_cache
def cached_organization(name: str) -> ExtractedOrganization:
    """Get the rki organization."""
    wikidata_organization = get_wikidata_organization(name)
    extracted_item = transform_wikidata_organization_to_extracted_organization(
        wikidata_organization,
        cached_primary_source(WIKIDATA_SOURCE_NAME).stableTargetId,
    )
    if not extracted_item:
        raise NoResultFoundError(name)
    connector = GraphConnector.get()
    deque(connector.ingest_items([extracted_item]))
    logger.info("ingested organization %s", name)
    return extracted_item


def cached_organizational_units() -> list[ExtractedOrganizationalUnit]:
    """Auxiliary function to get ldap as primary resource and ingest org units."""
    organigram_units = extract_organigram_units()
    organigram_primary_source = cached_primary_source(ORGANIGRAM_PRIMARY_SOURCE_NAME)
    extracted_units = transform_organigram_units_to_organizational_units(
        organigram_units,
        organigram_primary_source.stableTargetId,
        cached_organization(RKI_WIKIDATA_ID).stableTargetId,
    )
    if not extracted_units:
        raise NoResultFoundError
    connector = GraphConnector.get()
    deque(connector.ingest_items(extracted_units))
    logger.info("ingested organizational units")
    return extracted_units


class LDAPLookup[ItemT](Protocol):
    """A paginated `LDAPConnector` lookup with all filter arguments already bound."""

    def __call__(self, *, limit: int, offset: int) -> PaginatedItemsContainer[ItemT]:
        """Run the lookup for one page of results."""


def fetch_ldap_page[ItemT](
    lookup: LDAPLookup[ItemT], *, limit: int, offset: int
) -> PaginatedItemsContainer[ItemT]:
    """Fetch one page of an LDAP lookup, returning an empty page past the end.

    mex-common's `LDAPConnector` raises a `ValueError` when the offset is at or past
    the total number of matches. Request validation already rejects negative offsets,
    so here that error can only mean "past the end". In that case, repeat the lookup
    for the first item to learn the total and return an empty page with it, just like
    the graph-backed search endpoints do.

    Args:
        lookup: LDAP connector lookup with its filter arguments already bound
        limit: The maximum number of results to return
        offset: The number of results to skip before return

    Returns:
        Paginated container with the requested page and the total number of matches
    """
    try:
        return lookup(limit=limit, offset=offset)
    except ValueError:
        first_page = lookup(limit=1, offset=0)
        return PaginatedItemsContainer[ItemT](items=[], total=first_page.total)
