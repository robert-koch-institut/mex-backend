from typing import Any

import pytest
from fastapi.testclient import TestClient

from mex.backend.auxiliary.ldap import (
    extracted_organizational_unit,
    extracted_primary_source_ldap,
)
from mex.backend.graph.connector import GraphConnector
from mex.common.models import ExtractedOrganizationalUnit, ExtractedPrimarySource
from mex.common.types import (
    ExtractedOrganizationalUnitIdentifier,
    MergedOrganizationalUnitIdentifier,
    Text,
    TextLanguage,
)
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
        ("", 422, None),
        ("None-Existent", 200, 0),
    ],
    ids=[
        "Get existing Person with same name",
        "Get existing Person with unique name",
        "Get existing Person by givenname",
        "Empty Search String",
        "Non-existent string",
    ],
)
@pytest.mark.usefixtures("mocked_ldap")
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
                "memberOf": ["guUvX7rDQJIaMD8LbZV40E"],
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
                "memberOf": ["guUvX7rDQJIaMD8LbZV40E"],
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
                "memberOf": ["guUvX7rDQJIaMD8LbZV40E"],
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
    assert response.status_code == status_code
    if response.status_code == 200:
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
    graph = GraphConnector.get()
    ingested_primary_source = graph.fetch_extracted_items(
        "Active Directory",
        str(result.stableTargetId),
        ["ExtractedPrimarySource"],
        0,
        100,
    )
    assert ingested_primary_source["total"] == 1, get_graph()


def test_extracted_organizational_unit() -> None:
    expected_result = [
        ExtractedOrganizationalUnit(
            hadPrimarySource="ebs5siX85RkdrhBRlsYgRP",
            identifierInPrimarySource="child-unit",
            parentUnit=None,
            name=[
                Text(value="CHLD Unterabteilung", language=TextLanguage.DE),
                Text(value="C1: Sub Unit", language=TextLanguage.EN),
            ],
            alternativeName=[
                Text(value="CHLD", language=TextLanguage.EN),
                Text(value="C1 Sub-Unit", language=TextLanguage.EN),
                Text(value="C1 Unterabteilung", language=TextLanguage.DE),
            ],
            email=[],
            shortName=[Text(value="C1", language=TextLanguage.DE)],
            unitOf=[],
            website=[],
            entityType="ExtractedOrganizationalUnit",
            identifier=ExtractedOrganizationalUnitIdentifier("guPShhkQFQY1WcGQPwhBUK"),
            stableTargetId=MergedOrganizationalUnitIdentifier("eaWrYZHz9arDWHYYgJ3Jvd"),
        ),
        ExtractedOrganizationalUnit(
            hadPrimarySource="ebs5siX85RkdrhBRlsYgRP",
            identifierInPrimarySource="parent-unit",
            parentUnit=None,
            name=[
                Text(value="Abteilung", language=TextLanguage.DE),
                Text(value="Department", language=TextLanguage.EN),
            ],
            alternativeName=[
                Text(value="PRNT Abteilung", language=TextLanguage.DE),
                Text(value="PARENT Dept.", language=None),
            ],
            email=["pu@example.com", "PARENT@example.com"],
            shortName=[Text(value="PRNT", language=TextLanguage.DE)],
            unitOf=[],
            website=[],
            entityType="ExtractedOrganizationalUnit",
            identifier=ExtractedOrganizationalUnitIdentifier("boqrd9TaaNZWxr22sAIiMZ"),
            stableTargetId=MergedOrganizationalUnitIdentifier("g7U6F67JzQgCwy6SFIzoVH"),
        ),
        ExtractedOrganizationalUnit(
            hadPrimarySource="ebs5siX85RkdrhBRlsYgRP",
            identifierInPrimarySource="fg99",
            parentUnit=None,
            name=[
                Text(value="Fachgebiet 99", language=TextLanguage.DE),
                Text(value="Group 99", language=TextLanguage.EN),
            ],
            alternativeName=[Text(value="FG99", language=TextLanguage.DE)],
            email=["fg@example.com"],
            shortName=[Text(value="FG 99", language=TextLanguage.DE)],
            unitOf=[],
            website=[],
            entityType="ExtractedOrganizationalUnit",
            identifier=ExtractedOrganizationalUnitIdentifier("dK70rp439YPCkCedfr5Fa6"),
            stableTargetId=MergedOrganizationalUnitIdentifier("guUvX7rDQJIaMD8LbZV40E"),
        ),
    ]
    result = extracted_organizational_unit()
    for item in result:
        assert isinstance(item, ExtractedOrganizationalUnit)
    assert result == expected_result

    # check org units are ingested into graph
    graph = GraphConnector.get()
    ingested_org_unit_1 = graph.fetch_extracted_items(
        "CHLD Unterabteilung",
        "eaWrYZHz9arDWHYYgJ3Jvd",
        ["ExtractedOrganizationalUnit"],
        0,
        100,
    )
    assert ingested_org_unit_1["total"] == 1, get_graph()
    ingested_org_unit_2 = graph.fetch_extracted_items(
        "PRNT Abteilung",
        "g7U6F67JzQgCwy6SFIzoVH",
        ["ExtractedOrganizationalUnit"],
        0,
        100,
    )
    assert ingested_org_unit_2["total"] == 1, get_graph()
    ingested_org_unit_3 = graph.fetch_extracted_items(
        "Fachgebiet 99",
        "guUvX7rDQJIaMD8LbZV40E",
        ["ExtractedOrganizationalUnit"],
        0,
        100,
    )
    assert ingested_org_unit_3["total"] == 1, get_graph()
