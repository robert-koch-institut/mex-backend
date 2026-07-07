from collections import deque
from functools import lru_cache
from typing import TYPE_CHECKING

from mex.backend.auxiliary.constants import (
    ORGANIGRAM_PRIMARY_SOURCE_NAME,
    RKI_WIKIDATA_ID,
    WIKIDATA_SOURCE_NAME,
)
from mex.backend.graph.connector import GraphConnector
from mex.backend.graph.exceptions import NoResultFoundError
from mex.common.logging import logger
from mex.common.organigram.extract import extract_organigram_units
from mex.common.organigram.transform import (
    transform_organigram_units_to_organizational_units,
)
from mex.common.primary_source.helpers import get_extracted_primary_source_by_name
from mex.common.wikidata.extract import get_wikidata_organization
from mex.common.wikidata.transform import (
    transform_wikidata_organization_to_extracted_organization,
)

if TYPE_CHECKING:  # pragma: no cover
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
    if not organigram_units:
        raise NoResultFoundError
    organigram_primary_source = cached_primary_source(ORGANIGRAM_PRIMARY_SOURCE_NAME)
    extracted_units = transform_organigram_units_to_organizational_units(
        organigram_units,
        organigram_primary_source.stableTargetId,
        cached_organization(RKI_WIKIDATA_ID).stableTargetId,
    )
    connector = GraphConnector.get()
    deque(connector.ingest_items(extracted_units))
    return extracted_units
