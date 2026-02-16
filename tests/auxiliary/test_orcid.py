from typing import TYPE_CHECKING, Any

import pytest
from starlette import status

from mex.common.orcid.connector import OrcidConnector

if TYPE_CHECKING:  # pragma: no cover
    from fastapi.testclient import TestClient

john_doe_response = {
    "hadPrimarySource": "Naj2hOJq9FNRkkMWa5Qd0",
    "identifierInPrimarySource": "0009-0004-3041-5706",
    "affiliation": [],
    "email": [],
    "familyName": ["Doe"],
    "fullName": ["Doe, John"],
    "givenName": ["John"],
    "isniId": [],
    "memberOf": [],
    "orcidId": ["https://orcid.org/0009-0004-3041-5706"],
    "$type": "ExtractedPerson",
    "identifier": "eLFbvlVkwRgGxLnS8RywZ3",
    "stableTargetId": "ccM55btPUrNtYKSLX8cNQP",
}


@pytest.mark.parametrize(
    ("search_string", "expected"),
    [
        pytest.param(
            "John Doe",
            {"items": [john_doe_response], "total": 1},
            id="existing-person",
        ),
        pytest.param(
            "Multiple Doe",
            {"items": [john_doe_response] * 10, "total": 10},
            id="multiple-results",
        ),
        pytest.param(
            "John O'Doe",
            {"items": [john_doe_response], "total": 1},
            id="special-symbols",
        ),
        pytest.param(
            "",
            {"items": [], "total": 0},
            id="empty-search",
        ),
    ],
)
@pytest.mark.usefixtures("mocked_orcid")
def test_search_persons_in_orcid_mocked(
    client_with_api_key_read_permission: TestClient,
    search_string: str,
    expected: dict[str, Any],
) -> None:
    response = client_with_api_key_read_permission.get(
        "/v0/orcid", params={"q": search_string}
    )
    assert response.status_code == status.HTTP_200_OK, response.text
    assert response.json() == expected


@pytest.mark.usefixtures("mocked_orcid")
def test_search_persons_in_orcid_empty(
    client_with_api_key_read_permission: TestClient,
) -> None:
    response = client_with_api_key_read_permission.get(
        "/v0/orcid", params={"q": "none shall be found"}
    )
    assert response.status_code == status.HTTP_200_OK, response.text
    assert response.json() == {"items": [], "total": 0}


@pytest.mark.parametrize(
    ("filters", "expected_output"),
    [
        pytest.param(
            {"given-and-family-names": "'John Doe'"},
            "given-and-family-names:'John Doe'",
            id="one-param",
        ),
        pytest.param(
            {"given-names": "John", "family-name": "Doe"},
            "given-names:John AND family-name:Doe",
            id="two-params",
        ),
    ],
)
@pytest.mark.usefixtures("mocked_orcid")
def test_build_query(filters: dict[str, Any], expected_output: str) -> None:
    connector = OrcidConnector.get()
    built_query = connector.build_query(filters)
    assert built_query == expected_output
