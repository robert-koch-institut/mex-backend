from typing import cast

from mex.backend.extracted.helpers import search_extracted_items_in_graph
from mex.backend.graph.connector import GraphConnector
from mex.backend.graph.exceptions import NoResultFoundError
from mex.common.identity import get_provider
from mex.common.logging import logger
from mex.common.models import (
    MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
    ExtractedPrimarySource,
)
from mex.common.primary_source.helpers import get_extracted_primary_source_by_name


def _fetch_or_insert_primary_source(name: str) -> ExtractedPrimarySource:
    """Fetch and return or load, ingest and return a primary source by name."""
    provider = get_provider()
    identities = provider.fetch(
        had_primary_source=MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
        identifier_in_primary_source=name,
    )
    if identities:
        primary_source_container = search_extracted_items_in_graph(
            stable_target_id=identities[0].stableTargetId
        )
        if primary_sources := primary_source_container.items:
            return cast("list[ExtractedPrimarySource]", primary_sources)[0]

    extracted_primary_source = get_extracted_primary_source_by_name(name)
    if not extracted_primary_source:
        raise NoResultFoundError(name)
    connector = GraphConnector.get()
    connector.ingest([extracted_primary_source])
    logger.info("ingested primary source %s", name)
    return extracted_primary_source


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
