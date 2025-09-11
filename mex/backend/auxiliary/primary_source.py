from collections import deque
from typing import cast

from mex.backend.auxiliary.utils import fetch_extracted_item_by_source_identifiers
from mex.backend.graph.connector import GraphConnector
from mex.backend.graph.exceptions import NoResultFoundError
from mex.common.logging import logger
from mex.common.models import (
    MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
    ExtractedPrimarySource,
)
from mex.common.primary_source.helpers import get_extracted_primary_source_by_name


def _fetch_or_insert_primary_source(name: str) -> ExtractedPrimarySource:
    """Fetch and return or load, ingest and return a primary source by name."""
    extracted_item = fetch_extracted_item_by_source_identifiers(
        had_primary_source=MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
        identifier_in_primary_source=name,
    )
    if extracted_item:
        return cast("ExtractedPrimarySource", extracted_item)
    extracted_item = get_extracted_primary_source_by_name(name)
    if not extracted_item:
        raise NoResultFoundError(name)
    connector = GraphConnector.get()
    deque(connector.ingest_items([extracted_item]))
    logger.info("ingested primary source %s", name)
    return extracted_item


def extracted_primary_source_ldap() -> ExtractedPrimarySource:
    """Get the ldap primary source."""
    return _fetch_or_insert_primary_source("ldap")


def extracted_primary_source_organigram() -> ExtractedPrimarySource:
    """Get the organigram primary source."""
    return _fetch_or_insert_primary_source("organigram")


def extracted_primary_source_orcid() -> ExtractedPrimarySource:
    """Get the orcid primary source."""
    return _fetch_or_insert_primary_source("orcid")


def extracted_primary_source_wikidata() -> ExtractedPrimarySource:
    """Get the wikidata primary source."""
    return _fetch_or_insert_primary_source("wikidata")
