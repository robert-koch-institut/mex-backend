from typing import Annotated

from fastapi import APIRouter, Query

from mex.backend.auxiliary.organigram import extracted_organizational_unit
from mex.backend.auxiliary.primary_source import extracted_primary_source_ldap
from mex.common.ldap.extract import (
    get_count_of_found_persons_by_name,
    get_persons_by_name,
)
from mex.common.ldap.transform import transform_ldap_persons_to_mex_persons
from mex.common.models import ExtractedPerson, PaginatedItemsContainer

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
