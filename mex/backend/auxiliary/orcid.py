from typing import Annotated

from fastapi import APIRouter, Query

from mex.common.exceptions import EmptySearchResultError
from mex.common.models import ExtractedPerson, PaginatedItemsContainer
from mex.common.orcid.extract import (
    get_orcid_records_by_given_or_family_name,
)
from mex.common.orcid.transform import transform_orcid_person_to_mex_person

router = APIRouter()


@router.get("/orcid", tags=["editor"])
def search_person_in_orcid(
    q: Annotated[str, Query(min_length=1, max_length=1000)],
    offset: Annotated[int, Query(ge=0, le=10e10)] = 0,
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
) -> PaginatedItemsContainer[ExtractedPerson]:
    """Search for persons in orcid.

    Args:
        q: The name of the person to be searched.
        offset: The starting index for pagination
        limit: The maximum number of results to return

    Returns:
        Paginated list of ExtractedPersons
    """
    params = {"given_and_family_names": q} if q else {}
    try:
        orcid_records = list(get_orcid_records_by_given_or_family_name(**params))
    except EmptySearchResultError:
        orcid_records = []
    total_count = len(orcid_records)
    extracted_persons = [
        transform_orcid_person_to_mex_person(person)
        for person in orcid_records[offset : offset + limit]
    ]
    return PaginatedItemsContainer[ExtractedPerson](
        items=extracted_persons, total=total_count
    )
