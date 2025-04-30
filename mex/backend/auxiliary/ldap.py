from typing import Annotated

from fastapi import APIRouter, Query

from mex.backend.auxiliary.organigram import extracted_organizational_units
from mex.backend.auxiliary.primary_source import extracted_primary_source_ldap
from mex.common.ldap.extract import get_ldap_persons
from mex.common.ldap.transform import transform_ldap_persons_to_mex_persons
from mex.common.models import ExtractedPerson, PaginatedItemsContainer

router = APIRouter()


@router.get("/ldap", tags=["editor"])
def search_persons_in_ldap(
    q: Annotated[str, Query(max_length=1000)] = "A*",
    offset: Annotated[int, Query(ge=0, le=10e10)] = 0,  # noqa: ARG001
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
) -> PaginatedItemsContainer[ExtractedPerson]:
    """Search for persons in LDAP.

    Args:
        q: The name of the person to be searched
        offset: The starting index for pagination (not implemented)
        limit: The maximum number of results to return

    Returns:
        Paginated list of ExtractedPersons
    """
    ldap_persons = get_ldap_persons(
        limit=limit,
        displayName=q,
    )
    extracted_persons = transform_ldap_persons_to_mex_persons(
        ldap_persons,
        extracted_primary_source_ldap(),
        extracted_organizational_units(),
    )
    return PaginatedItemsContainer[ExtractedPerson](
        items=extracted_persons, total=len(ldap_persons)
    )
