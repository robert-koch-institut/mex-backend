from typing import Annotated

from fastapi import APIRouter, HTTPException, Query

from mex.backend.auxiliary.models import AuxiliarySearch
from mex.common.exceptions import EmptySearchResultError
from mex.common.models import (
    ExtractedPerson,
)
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
) -> AuxiliarySearch[ExtractedPerson]:
    """Search for persons in orcid by string.

    Args:
        q: The name of the person to be searched.
        offset: The starting index for pagination
        limit: The maximum number of results to return

    Returns:
        ExtractedPerson and the total count of persons found.
    """
    try:
        orcid_records = list(
            get_orcid_records_by_given_or_family_name(given_and_family_names=q)
        )

    except EmptySearchResultError as e:
        raise HTTPException(
            status_code=404, detail=f"No results found for '{q}'."
        ) from e
    total_results = len(orcid_records)
    paginated_persons = orcid_records[offset : offset + limit]
    extracted_persons = [
        transform_orcid_person_to_mex_person(person) for person in paginated_persons
    ]
    return AuxiliarySearch(items=extracted_persons, total=total_results)
