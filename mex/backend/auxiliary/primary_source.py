from functools import cache

from mex.backend.graph.connector import GraphConnector
from mex.common.models import ExtractedPrimarySource
from mex.common.primary_source.extract import extract_seed_primary_sources
from mex.common.primary_source.transform import (
    get_primary_sources_by_name,
    transform_seed_primary_sources_to_extracted_primary_sources,
)


@cache
def extracted_primary_sources() -> list[ExtractedPrimarySource]:
    """Return the list of seeded extracted primary sources."""
    return list(
        transform_seed_primary_sources_to_extracted_primary_sources(
            extract_seed_primary_sources()
        )
    )


@cache
def extracted_primary_source_ldap() -> ExtractedPrimarySource:
    """Load, ingest and return ldap primary source."""
    (extracted_primary_source_ldap,) = get_primary_sources_by_name(
        extracted_primary_sources(),
        "ldap",
    )
    connector = GraphConnector.get()
    connector.ingest([extracted_primary_source_ldap])
    return extracted_primary_source_ldap


@cache
def extracted_primary_source_orcid() -> ExtractedPrimarySource:
    """Load, ingest and return orcid primary source."""
    (extracted_primary_source_orcid,) = get_primary_sources_by_name(
        extracted_primary_sources(),
        "orcid",
    )
    connector = GraphConnector.get()
    connector.ingest([extracted_primary_source_orcid])
    return extracted_primary_source_orcid


@cache
def extracted_primary_source_wikidata() -> ExtractedPrimarySource:
    """Load, ingest and return wikidata primary source."""
    (extracted_primary_source_wikidata,) = get_primary_sources_by_name(
        extracted_primary_sources(),
        "wikidata",
    )
    connector = GraphConnector.get()
    connector.ingest([extracted_primary_source_wikidata])
    return extracted_primary_source_wikidata
