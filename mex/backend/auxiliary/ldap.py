from functools import cache
from typing import Annotated

from fastapi import APIRouter, Query

from mex.backend.graph.connector import GraphConnector
from mex.common.ldap.extract import (
    get_count_of_found_persons_by_name,
    get_persons_by_name,
)
from mex.common.ldap.transform import transform_ldap_persons_to_mex_persons
from mex.common.models import (
    ExtractedOrganizationalUnit,
    ExtractedPerson,
    ExtractedPrimarySource,
    PaginatedItemsContainer,
)
from mex.common.organigram.extract import extract_organigram_units
from mex.common.organigram.transform import (
    transform_organigram_units_to_organizational_units,
)
from mex.common.primary_source.extract import extract_seed_primary_sources
from mex.common.primary_source.transform import (
    get_primary_sources_by_name,
    transform_seed_primary_sources_to_extracted_primary_sources,
)

router = APIRouter()


@router.get("/ldap", tags=["editor"])
def search_person_in_ldap(
    q: Annotated[str, Query(min_length=1, max_length=1000)],
    offset: Annotated[int, Query(ge=0, le=10e10)] = 0,
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
) -> PaginatedItemsContainer[ExtractedPerson]:
    """Search for persons in LDAP.

    Args:
        q: The name of the person to be searched.
        offset: The starting index for pagination
        limit: The maximum number of results to return

    Returns:
        Paginated list of ExtractedPersons
    """
    params = {"display_name": q} if q else {}
    total_count = get_count_of_found_persons_by_name(**params)
    ldap_persons = list(get_persons_by_name(**params))
    paginated_persons = ldap_persons[offset : offset + limit]
    extracted_persons = list(
        transform_ldap_persons_to_mex_persons(
            paginated_persons,
            extracted_primary_source_ldap(),
            extracted_organizational_unit(),
        )
    )
    return PaginatedItemsContainer[ExtractedPerson](
        items=extracted_persons, total=total_count
    )


@cache
def extracted_primary_source_ldap() -> ExtractedPrimarySource:
    """Load, ingest and return ldap primary source."""
    seed_primary_sources = extract_seed_primary_sources()
    extracted_primary_sources = list(
        transform_seed_primary_sources_to_extracted_primary_sources(
            seed_primary_sources
        )
    )
    (extracted_primary_source_ldap,) = get_primary_sources_by_name(
        extracted_primary_sources,
        "ldap",
    )
    connector = GraphConnector.get()
    connector.ingest([extracted_primary_source_ldap])
    return extracted_primary_source_ldap


@cache
def extracted_organizational_unit() -> list[ExtractedOrganizationalUnit]:
    """Auxiliary function to get ldap as primary resource and ingest org units."""
    extracted_organigram_units = extract_organigram_units()
    extracted_organizational_units = transform_organigram_units_to_organizational_units(
        extracted_organigram_units, extracted_primary_source_ldap()
    )
    connector = GraphConnector.get()
    list_of_extracted_organizational_units = list(extracted_organizational_units)
    connector.ingest(list_of_extracted_organizational_units)  # type: ignore [arg-type]
    return list_of_extracted_organizational_units
