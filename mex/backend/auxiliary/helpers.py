from collections import deque
from functools import lru_cache
from typing import TYPE_CHECKING

from mex.backend.auxiliary.constants import (
    ORGANIGRAM_PRIMARY_SOURCE_NAME,
    RKI_WIKIDATA_ID,
)
from mex.backend.graph.connector import GraphConnector
from mex.backend.graph.exceptions import NoResultFoundError
from mex.common.logging import logger
from mex.common.organigram.extract import extract_organigram_units
from mex.common.organigram.transform import (
    transform_organigram_units_to_organizational_units,
)
from mex.common.primary_source.helpers import get_extracted_primary_source_by_name
from mex.common.wikidata.helpers import get_wikidata_organization_with_locations

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
    result = get_wikidata_organization_with_locations(name)

    if result is None:
        raise NoResultFoundError(name)

    connector = GraphConnector.get()
    deque(
        connector.ingest_items(
            [result.primary_source, *result.locations, result.organization]
        )
    )

    logger.info("ingested organization %s", name)
    return result.organization


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
