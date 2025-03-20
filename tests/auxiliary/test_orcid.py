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
        ("john doe", 200),
        ("John O'Doe", 200),
        ("", 422),
    ],
    ids=[
        "existing Person by name",
        "case insensitiv",
        "special symbols",
        "empty search String",
    ],
)
@pytest.mark.usefixtures("mocked_orcid")
def test_search_persons_in_orcid_mocked(
    client_with_api_key_read_permission: TestClient,
    search_string,
    status_code,
) -> None:
    response = client_with_api_key_read_permission.get(
        "/v0/orcid", params={"q": search_string}
    )
    assert response.status_code == status_code

    if response.status_code == 200:
        response_data = response.json()
        assert response_data["total"] == len(response_data["items"])
        assert response_data["items"] == [john_doe_response]
    elif response.status_code == 422:
        response_data = response.json()
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


@pytest.mark.parametrize(
    ("search_string", "expected_status"),
    [("None Doe", 404), ("Multiple Doe", 400)],
    ids=["Non-existent", "Multiple Results"],
)
@pytest.mark.usefixtures("mocked_orcid")
def test_search_persons_in_orcid_errors(
    client_with_api_key_read_permission: TestClient, search_string, expected_status
):
    response = client_with_api_key_read_permission.get(
        "/v0/orcid", params={"q": search_string}
    )
    assert response.status_code == expected_status
    if response.status_code == 404:
        assert response.text == '{"detail":"No results found for \'None Doe\'."}'
    else:
        assert (
            response.text == '{"detail":"Multiple persons found for \'Multiple Doe\'."}'
        )


@pytest.mark.parametrize(
    ("filters", "expected_output"),
    [
        (
            {"given-and-family-names": "'John Doe'"},
            "given-and-family-names:\"'John Doe'\"",
        ),
        (
            {"given-names": "John", "family-name": "Doe"},
            "given-names:John AND family-name:Doe",
        ),
    ],
    ids=["Non-existent", "Multiple Results"],
)
@pytest.mark.usefixtures("mocked_orcid")
def test_build_query(filters: dict[str, Any], expected_output: str):
    built_query = OrcidConnector.build_query(filters)
    assert built_query == expected_output
