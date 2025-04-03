from functools import cache

from mex.backend.auxiliary.primary_source import extracted_primary_source_ldap
from mex.backend.graph.connector import GraphConnector
from mex.common.models import ExtractedOrganizationalUnit
from mex.common.organigram.extract import extract_organigram_units
from mex.common.organigram.transform import (
    transform_organigram_units_to_organizational_units,
)


@cache
def extracted_organizational_unit() -> list[ExtractedOrganizationalUnit]:
    """Auxiliary function to get ldap as primary resource and ingest org units."""
    extracted_organizational_units = list(
        transform_organigram_units_to_organizational_units(
            extract_organigram_units(), extracted_primary_source_ldap()
        )
    )
    connector = GraphConnector.get()
    connector.ingest(extracted_organizational_units)
    return extracted_organizational_units
