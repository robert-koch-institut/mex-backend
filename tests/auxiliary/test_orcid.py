from typing import Any

import pytest
from fastapi.testclient import TestClient

from mex.common.orcid.connector import OrcidConnector

john_doe_response = {
    "hadPrimarySource": "Naj2hOJq9FNRkkMWa5Qd0",
    "identifierInPrimarySource": "0009-0004-3041-5706",
    "affiliation": [],
    "email": [],
    "familyName": ["Doe"],
    "fullName": [],
    "givenName": ["John"],
    "isniId": [],
    "memberOf": [],
    "orcidId": ["https://orcid.org/0009-0004-3041-5706"],
    "$type": "ExtractedPerson",
    "identifier": "eLFbvlVkwRgGxLnS8RywZ3",
    "stableTargetId": "ccM55btPUrNtYKSLX8cNQP",
}


@pytest.mark.parametrize(
    ("search_string", "status_code"),
    [
        ("John Doe", 200),
        ("Multiple Doe", 200),
        ("John O'Doe", 200),
        ("", 422),
    ],
    ids=[
        "existing person by name",
        "multiple results",
        "special symbols",
        "empty search string",
    ],
)
@pytest.mark.usefixtures("mocked_orcid")
def test_search_persons_in_orcid_mocked(
    client_with_api_key_read_permission: TestClient,
    search_string: str,
    status_code: int,
) -> None:
    response = client_with_api_key_read_permission.get(
        "/v0/orcid", params={"q": search_string}
    )
    assert response.status_code == status_code
    response_data = response.json()

    if response.status_code == 200:
        assert response_data["total"] == len(response_data["items"])
        if search_string == "Multiple Doe":
            assert response_data["items"] == [john_doe_response] * 10
        else:
            assert response_data["items"] == [john_doe_response]
    elif response.status_code == 422:
        assert response_data == {
            "detail": [
                {
                    "type": "string_too_short",
                    "loc": ["query", "q"],
                    "msg": "String should have at least 1 character",
                    "input": "",
                    "ctx": {"min_length": 1},
                }
            ]
        }


@pytest.mark.usefixtures("mocked_orcid")
def test_search_persons_in_orcid_empty(
    client_with_api_key_read_permission: TestClient,
) -> None:
    response = client_with_api_key_read_permission.get(
        "/v0/orcid", params={"q": "none shall be found"}
    )
    assert response.status_code == 200
    assert response.json() == {"items": [], "total": 0}


@pytest.mark.parametrize(
    ("filters", "expected_output"),
    [
        (
            {"given-and-family-names": "'John Doe'"},
            "given-and-family-names:'John Doe'",
        ),
        (
            {"given-names": "John", "family-name": "Doe"},
            "given-names:John AND family-name:Doe",
        ),
    ],
    ids=["One param", "two params"],
)
@pytest.mark.usefixtures("mocked_orcid")
def test_build_query(filters: dict[str, Any], expected_output: str) -> None:
    connector = OrcidConnector.get()
    built_query = connector.build_query(filters)
    assert built_query == expected_output
