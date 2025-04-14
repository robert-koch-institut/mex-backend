from functools import cache

from mex.backend.graph.connector import GraphConnector
from mex.common.models import ExtractedPrimarySource
from mex.common.primary_source.helpers import get_all_extracted_primary_sources


@cache
def extracted_primary_source_ldap() -> ExtractedPrimarySource:
    """Load, ingest and return ldap primary source."""
    extracted_primary_source = get_all_extracted_primary_sources()["ldap"]
    connector = GraphConnector.get()
    connector.ingest([extracted_primary_source])
    return extracted_primary_source


@cache
def extracted_primary_source_orcid() -> ExtractedPrimarySource:
    """Load, ingest and return orcid primary source."""
    extracted_primary_source = get_all_extracted_primary_sources()["orcid"]
    connector = GraphConnector.get()
    connector.ingest([extracted_primary_source])
    return extracted_primary_source


@cache
def extracted_primary_source_wikidata() -> ExtractedPrimarySource:
    """Load, ingest and return wikidata primary source."""
    extracted_primary_source = get_all_extracted_primary_sources()["wikidata"]
    connector = GraphConnector.get()
    connector.ingest([extracted_primary_source])
    return extracted_primary_source
