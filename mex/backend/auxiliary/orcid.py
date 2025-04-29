from typing import Annotated

from fastapi import APIRouter, Query

from mex.backend.auxiliary.primary_source import extracted_primary_source_orcid
from mex.common.models import ExtractedPerson, PaginatedItemsContainer
from mex.common.orcid.extract import search_records_by_name
from mex.common.orcid.transform import transform_orcid_person_to_mex_person

router = APIRouter()


@router.get("/orcid", tags=["editor"])
def search_persons_in_orcid(
    q: Annotated[str, Query(max_length=1000)] = "Robert Koch",
    offset: Annotated[int, Query(ge=0, le=10e10)] = 0,
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
) -> PaginatedItemsContainer[ExtractedPerson]:
    """Search for persons in orcid.

    Args:
        q: The name of the person to be searched
        offset: The starting index for pagination
        limit: The maximum number of results to return

    Returns:
        Paginated list of ExtractedPersons
    """
    orcid_records = search_records_by_name(
        given_and_family_names=q,
        skip=offset,
        limit=limit,
    )
    extracted_persons = [
        transform_orcid_person_to_mex_person(
            person,
            extracted_primary_source_orcid(),
        )
        for person in orcid_records.items
    ]
    return PaginatedItemsContainer[ExtractedPerson](
        items=extracted_persons, total=orcid_records.total
    )
