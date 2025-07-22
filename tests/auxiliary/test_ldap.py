from typing import Any

import pytest
from fastapi.testclient import TestClient

from mex.backend.auxiliary.primary_source import extracted_primary_source_ldap
from mex.backend.extracted.helpers import search_extracted_items_in_graph
from mex.common.models import ExtractedPrimarySource
from mex.common.types import TextLanguage
from tests.conftest import get_graph


def count_results(search_string: str, persons: list[dict[str, Any]]) -> int:
    return sum(
        1
        for person in persons
        if search_string in person.get("givenName", [])
        or search_string in person.get("familyName", [])
    )


@pytest.mark.parametrize(
    ("search_string", "status_code", "match_total"),
    [
        ("Mueller", 200, 2),
        ("Example", 200, 1),
        ("Moritz", 200, 2),
        ("", 200, 0),
        ("None-Existent", 200, 0),
    ],
    ids=[
        "Get existing Person with same name",
        "Get existing Person with unique name",
        "Get existing Person by given name",
        "Empty Search String",
        "Non-existent string",
    ],
)
@pytest.mark.usefixtures("mocked_ldap", "mocked_wikidata")
def test_search_persons_in_ldap_mocked(
    client_with_api_key_read_permission: TestClient,
    search_string: str,
    status_code: int,
    match_total: int,
) -> None:
    result = {
        "items": [
            {
                "hadPrimarySource": "ebs5siX85RkdrhBRlsYgRP",
                "identifierInPrimarySource": "00000000-0000-4000-8000-000000000001",
                "affiliation": [],
                "email": [],
                "familyName": ["Mueller"],
                "fullName": [],
                "givenName": ["Max"],
                "isniId": [],
                "memberOf": ["cjna2jitPngp6yIV63cdi9"],
                "orcidId": [],
                "$type": "ExtractedPerson",
                "identifier": "uNaZQCzsY4IAJ2ahzRBQX",
                "stableTargetId": "eXA2Qj5pKmI7HXIgcVqCfz",
            },
            {
                "hadPrimarySource": "ebs5siX85RkdrhBRlsYgRP",
                "identifierInPrimarySource": "00000000-0000-4000-8000-000000000002",
                "affiliation": [],
                "email": [],
                "familyName": ["Example"],
                "fullName": [],
                "givenName": ["Moritz"],
                "isniId": [],
                "memberOf": ["cjna2jitPngp6yIV63cdi9"],
                "orcidId": [],
                "$type": "ExtractedPerson",
                "identifier": "hiY0YTC5dUkBrf8ujMemjh",
                "stableTargetId": "cpKNwpoZTQ4GpIzBgO8DMx",
            },
            {
                "hadPrimarySource": "ebs5siX85RkdrhBRlsYgRP",
                "identifierInPrimarySource": "00000000-0000-4000-8000-000000000003",
                "affiliation": [],
                "email": [],
                "familyName": ["Mueller"],
                "fullName": [],
                "givenName": ["Moritz"],
                "isniId": [],
                "memberOf": ["cjna2jitPngp6yIV63cdi9"],
                "orcidId": [],
                "$type": "ExtractedPerson",
                "identifier": "b9Dw7MmjFtoFd6upaaKQHB",
                "stableTargetId": "c2Yd8aNoLKIf7u6ubTUuc3",
            },
        ],
        "total": 3,
    }
    response = client_with_api_key_read_permission.get(
        "/v0/ldap", params={"q": search_string}
    )
    assert response.status_code == status_code, response.text
    data = response.json()
    assert data == result
    assert count_results(search_string, data["items"]) == match_total


def test_extracted_primary_source_ldap() -> None:
    result = extracted_primary_source_ldap()
    assert isinstance(result, ExtractedPrimarySource)
    assert result.model_dump() == {
        "hadPrimarySource": "00000000000000",
        "identifierInPrimarySource": "ldap",
        "version": None,
        "alternativeTitle": [],
        "contact": [],
        "description": [],
        "documentation": [],
        "locatedAt": [],
        "title": [{"value": "Active Directory", "language": TextLanguage.EN}],
        "unitInCharge": [],
        "entityType": "ExtractedPrimarySource",
        "identifier": "cmiaN880A6fm1Ggno4kl7m",
        "stableTargetId": "ebs5siX85RkdrhBRlsYgRP",
    }


@pytest.mark.integration
def test_extracted_primary_source_ldap_ingest() -> None:
    # verify the primary source ldap has been stored in the database
    result = extracted_primary_source_ldap()

    ingested = search_extracted_items_in_graph(
        query_string="Active Directory",
        stable_target_id=str(result.stableTargetId),
        entity_type=["ExtractedPrimarySource"],
    )

    assert ingested.total == 1, get_graph()
