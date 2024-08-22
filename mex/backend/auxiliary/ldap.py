from functools import cache
from typing import Annotated

from fastapi import APIRouter, Query
from pydantic import PlainSerializer

from mex.backend.auxiliary.models import AuxiliarySearch
from mex.backend.serialization import to_primitive
from mex.common.models import ExtractedPerson, ExtractedPrimarySource
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
) -> Annotated[AuxiliarySearch[ExtractedPerson], PlainSerializer(to_primitive)]:
    return AuxiliarySearch()


@cache
def extracted_primary_source_ldap() -> ExtractedPrimarySource:
    """Load and return ldap primary source."""
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
    return extracted_primary_source_ldap
