from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient
from pytest import MonkeyPatch
from starlette import status

from mex.backend.graph.connector import GraphConnector
from mex.common.models import (
    MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
    AdditivePerson,
    AnyExtractedModel,
    ExtractedContactPoint,
    ExtractedPerson,
    OrganizationalUnitRuleSetResponse,
    PersonRuleSetResponse,
    SubtractivePerson,
)
from tests.conftest import get_graph


@pytest.mark.integration
def test_ingest_empty(client_with_api_key_write_permission: TestClient) -> None:
    response = client_with_api_key_write_permission.post(
        "/v0/ingest", json={"items": []}
    )
    assert response.status_code == status.HTTP_204_NO_CONTENT, response.text
    assert response.text == ""


@pytest.mark.integration
def test_ingest_extracted(
    client_with_api_key_write_permission: TestClient,
    dummy_data: dict[str, AnyExtractedModel],
) -> None:
    # post the artificial data to the ingest endpoint
    response = client_with_api_key_write_permission.post(
        "/v0/ingest", json={"items": list(dummy_data.values())}
    )

    # assert the request was accepted
    assert response.status_code == status.HTTP_204_NO_CONTENT, response.text

    # verify the nodes have actually been stored in the database
    assert get_graph() == [
        {
            "fundingProgram": [],
            "identifierInPrimarySource": "a-1",
            "start": ["2014-08-24"],
            "theme": ["https://mex.rki.de/item/theme-11"],
            "label": "ExtractedActivity",
            "activityType": [],
            "identifier": "bFQoRhcVH5DHUG",
            "end": [],
        },
        {
            "identifierInPrimarySource": "cp-2",
            "email": ["help@contact-point.two"],
            "label": "ExtractedContactPoint",
            "identifier": "bFQoRhcVH5DHUA",
        },
        {
            "identifierInPrimarySource": "cp-1",
            "email": ["info@contact-point.one"],
            "label": "ExtractedContactPoint",
            "identifier": "bFQoRhcVH5DHUy",
        },
        {
            "identifierInPrimarySource": "ou-1.6",
            "email": [],
            "label": "ExtractedOrganizationalUnit",
            "identifier": "bFQoRhcVH5DHUE",
        },
        {
            "identifierInPrimarySource": "ou-1",
            "email": [],
            "label": "ExtractedOrganizationalUnit",
            "identifier": "bFQoRhcVH5DHUw",
        },
        {
            "position": 0,
            "start": "00000000000001",
            "label": "hadPrimarySource",
            "end": "00000000000000",
        },
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUq",
            "label": "hadPrimarySource",
            "end": "00000000000000",
        },
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUs",
            "label": "hadPrimarySource",
            "end": "00000000000000",
        },
        {
            "position": 0,
            "start": "00000000000001",
            "label": "stableTargetId",
            "end": "00000000000000",
        },
        {"position": 0, "start": "bFQoRhcVH5DHUG", "label": "website", "end": "Link"},
        {"position": 0, "start": "bFQoRhcVH5DHUG", "label": "abstract", "end": "Text"},
        {"position": 1, "start": "bFQoRhcVH5DHUG", "label": "abstract", "end": "Text"},
        {"position": 0, "start": "bFQoRhcVH5DHUE", "label": "name", "end": "Text"},
        {"position": 0, "start": "bFQoRhcVH5DHUw", "label": "name", "end": "Text"},
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUC",
            "label": "officialName",
            "end": "Text",
        },
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUu",
            "label": "officialName",
            "end": "Text",
        },
        {
            "position": 1,
            "start": "bFQoRhcVH5DHUC",
            "label": "officialName",
            "end": "Text",
        },
        {
            "position": 1,
            "start": "bFQoRhcVH5DHUu",
            "label": "officialName",
            "end": "Text",
        },
        {"position": 0, "start": "bFQoRhcVH5DHUG", "label": "title", "end": "Text"},
        {
            "position": 1,
            "start": "bFQoRhcVH5DHUG",
            "label": "contact",
            "end": "bFQoRhcVH5DHUB",
        },
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUA",
            "label": "stableTargetId",
            "end": "bFQoRhcVH5DHUB",
        },
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUC",
            "label": "stableTargetId",
            "end": "bFQoRhcVH5DHUD",
        },
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUE",
            "label": "stableTargetId",
            "end": "bFQoRhcVH5DHUF",
        },
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUG",
            "label": "stableTargetId",
            "end": "bFQoRhcVH5DHUH",
        },
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUA",
            "label": "hadPrimarySource",
            "end": "bFQoRhcVH5DHUr",
        },
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUG",
            "label": "hadPrimarySource",
            "end": "bFQoRhcVH5DHUr",
        },
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUu",
            "label": "hadPrimarySource",
            "end": "bFQoRhcVH5DHUr",
        },
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUy",
            "label": "hadPrimarySource",
            "end": "bFQoRhcVH5DHUr",
        },
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUq",
            "label": "stableTargetId",
            "end": "bFQoRhcVH5DHUr",
        },
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUC",
            "label": "hadPrimarySource",
            "end": "bFQoRhcVH5DHUt",
        },
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUE",
            "label": "hadPrimarySource",
            "end": "bFQoRhcVH5DHUt",
        },
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUw",
            "label": "hadPrimarySource",
            "end": "bFQoRhcVH5DHUt",
        },
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUs",
            "label": "stableTargetId",
            "end": "bFQoRhcVH5DHUt",
        },
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUu",
            "label": "stableTargetId",
            "end": "bFQoRhcVH5DHUv",
        },
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUE",
            "label": "unitOf",
            "end": "bFQoRhcVH5DHUv",
        },
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUw",
            "label": "unitOf",
            "end": "bFQoRhcVH5DHUv",
        },
        {
            "position": 2,
            "start": "bFQoRhcVH5DHUG",
            "label": "contact",
            "end": "bFQoRhcVH5DHUx",
        },
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUE",
            "label": "parentUnit",
            "end": "bFQoRhcVH5DHUx",
        },
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUG",
            "label": "responsibleUnit",
            "end": "bFQoRhcVH5DHUx",
        },
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUw",
            "label": "stableTargetId",
            "end": "bFQoRhcVH5DHUx",
        },
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUG",
            "label": "contact",
            "end": "bFQoRhcVH5DHUz",
        },
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUy",
            "label": "stableTargetId",
            "end": "bFQoRhcVH5DHUz",
        },
        {
            "rorId": [],
            "identifierInPrimarySource": "robert-koch-institute",
            "gndId": [],
            "wikidataId": [],
            "geprisId": [],
            "viafId": [],
            "isniId": [],
            "label": "ExtractedOrganization",
            "identifier": "bFQoRhcVH5DHUC",
        },
        {
            "rorId": [],
            "identifierInPrimarySource": "rki",
            "gndId": [],
            "wikidataId": [],
            "geprisId": [],
            "viafId": [],
            "isniId": [],
            "label": "ExtractedOrganization",
            "identifier": "bFQoRhcVH5DHUu",
        },
        {"label": "MergedPrimarySource", "identifier": "00000000000000"},
        {
            "identifierInPrimarySource": "mex",
            "label": "ExtractedPrimarySource",
            "identifier": "00000000000001",
        },
        {"label": "MergedContactPoint", "identifier": "bFQoRhcVH5DHUB"},
        {"label": "MergedOrganization", "identifier": "bFQoRhcVH5DHUD"},
        {"label": "MergedOrganizationalUnit", "identifier": "bFQoRhcVH5DHUF"},
        {"label": "MergedActivity", "identifier": "bFQoRhcVH5DHUH"},
        {
            "identifierInPrimarySource": "ps-1",
            "label": "ExtractedPrimarySource",
            "identifier": "bFQoRhcVH5DHUq",
        },
        {"label": "MergedPrimarySource", "identifier": "bFQoRhcVH5DHUr"},
        {
            "identifierInPrimarySource": "ps-2",
            "label": "ExtractedPrimarySource",
            "identifier": "bFQoRhcVH5DHUs",
            "version": "Cool Version v2.13",
        },
        {"label": "MergedPrimarySource", "identifier": "bFQoRhcVH5DHUt"},
        {"label": "MergedOrganization", "identifier": "bFQoRhcVH5DHUv"},
        {"label": "MergedOrganizationalUnit", "identifier": "bFQoRhcVH5DHUx"},
        {"label": "MergedContactPoint", "identifier": "bFQoRhcVH5DHUz"},
        {"title": "Activity Homepage", "label": "Link", "url": "https://activity-1"},
        {"value": "Aktivität 1", "label": "Text", "language": "de"},
        {"value": "RKI", "label": "Text", "language": "de"},
        {"value": "RKI", "label": "Text", "language": "de"},
        {
            "value": "Robert Koch Institut ist the best",
            "label": "Text",
            "language": "de",
        },
        {"value": "An active activity.", "label": "Text", "language": "en"},
        {"value": "Robert Koch Institute", "label": "Text", "language": "en"},
        {"value": "Unit 1", "label": "Text", "language": "en"},
        {"value": "Unit 1.6", "label": "Text", "language": "en"},
        {"value": "Eng aktiv Aktivitéit.", "label": "Text"},
    ]


@pytest.mark.integration
@pytest.mark.usefixtures("load_dummy_data")
def test_ingest_rule_set(
    client_with_api_key_write_permission: TestClient,
    organizational_unit_rule_set_response: OrganizationalUnitRuleSetResponse,
) -> None:
    # post the rule set to the ingest endpoint
    response = client_with_api_key_write_permission.post(
        "/v0/ingest", json={"items": [organizational_unit_rule_set_response]}
    )

    # assert the request was accepted
    assert response.status_code == status.HTTP_204_NO_CONTENT, response.text

    # verify the nodes have actually been stored in the database
    assert get_graph() == [
        {
            "fundingProgram": [],
            "identifierInPrimarySource": "a-1",
            "start": ["2014-08-24"],
            "theme": ["https://mex.rki.de/item/theme-11"],
            "label": "ExtractedActivity",
            "activityType": [],
            "identifier": "bFQoRhcVH5DHUG",
            "end": [],
        },
        {
            "identifierInPrimarySource": "cp-2",
            "email": ["help@contact-point.two"],
            "label": "ExtractedContactPoint",
            "identifier": "bFQoRhcVH5DHUA",
        },
        {
            "identifierInPrimarySource": "cp-1",
            "email": ["info@contact-point.one"],
            "label": "ExtractedContactPoint",
            "identifier": "bFQoRhcVH5DHUy",
        },
        {
            "identifierInPrimarySource": "ou-1.6",
            "email": [],
            "label": "ExtractedOrganizationalUnit",
            "identifier": "bFQoRhcVH5DHUE",
        },
        {
            "identifierInPrimarySource": "ou-1",
            "email": [],
            "label": "ExtractedOrganizationalUnit",
            "identifier": "bFQoRhcVH5DHUw",
        },
        {"email": [], "label": "AdditiveOrganizationalUnit"},
        {"email": [], "label": "SubtractiveOrganizationalUnit"},
        {
            "position": 0,
            "start": "00000000000001",
            "label": "hadPrimarySource",
            "end": "00000000000000",
        },
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUq",
            "label": "hadPrimarySource",
            "end": "00000000000000",
        },
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUs",
            "label": "hadPrimarySource",
            "end": "00000000000000",
        },
        {
            "position": 0,
            "start": "00000000000001",
            "label": "stableTargetId",
            "end": "00000000000000",
        },
        {
            "position": 0,
            "start": "AdditiveOrganizationalUnit",
            "label": "website",
            "end": "Link",
        },
        {"position": 0, "start": "bFQoRhcVH5DHUG", "label": "website", "end": "Link"},
        {"position": 0, "start": "bFQoRhcVH5DHUG", "label": "abstract", "end": "Text"},
        {"position": 1, "start": "bFQoRhcVH5DHUG", "label": "abstract", "end": "Text"},
        {
            "position": 0,
            "start": "AdditiveOrganizationalUnit",
            "label": "name",
            "end": "Text",
        },
        {"position": 0, "start": "bFQoRhcVH5DHUE", "label": "name", "end": "Text"},
        {"position": 0, "start": "bFQoRhcVH5DHUw", "label": "name", "end": "Text"},
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUC",
            "label": "officialName",
            "end": "Text",
        },
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUu",
            "label": "officialName",
            "end": "Text",
        },
        {
            "position": 1,
            "start": "bFQoRhcVH5DHUC",
            "label": "officialName",
            "end": "Text",
        },
        {
            "position": 1,
            "start": "bFQoRhcVH5DHUu",
            "label": "officialName",
            "end": "Text",
        },
        {"position": 0, "start": "bFQoRhcVH5DHUG", "label": "title", "end": "Text"},
        {
            "position": 0,
            "start": "AdditiveOrganizationalUnit",
            "label": "stableTargetId",
            "end": "bFQoRhcVH5DHU6",
        },
        {
            "position": 0,
            "start": "PreventiveOrganizationalUnit",
            "label": "stableTargetId",
            "end": "bFQoRhcVH5DHU6",
        },
        {
            "position": 0,
            "start": "SubtractiveOrganizationalUnit",
            "label": "stableTargetId",
            "end": "bFQoRhcVH5DHU6",
        },
        {
            "position": 1,
            "start": "bFQoRhcVH5DHUG",
            "label": "contact",
            "end": "bFQoRhcVH5DHUB",
        },
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUA",
            "label": "stableTargetId",
            "end": "bFQoRhcVH5DHUB",
        },
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUE",
            "label": "stableTargetId",
            "end": "bFQoRhcVH5DHUF",
        },
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUG",
            "label": "stableTargetId",
            "end": "bFQoRhcVH5DHUH",
        },
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUA",
            "label": "hadPrimarySource",
            "end": "bFQoRhcVH5DHUr",
        },
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUG",
            "label": "hadPrimarySource",
            "end": "bFQoRhcVH5DHUr",
        },
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUu",
            "label": "hadPrimarySource",
            "end": "bFQoRhcVH5DHUr",
        },
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUy",
            "label": "hadPrimarySource",
            "end": "bFQoRhcVH5DHUr",
        },
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUq",
            "label": "stableTargetId",
            "end": "bFQoRhcVH5DHUr",
        },
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUC",
            "label": "hadPrimarySource",
            "end": "bFQoRhcVH5DHUt",
        },
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUE",
            "label": "hadPrimarySource",
            "end": "bFQoRhcVH5DHUt",
        },
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUw",
            "label": "hadPrimarySource",
            "end": "bFQoRhcVH5DHUt",
        },
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUs",
            "label": "stableTargetId",
            "end": "bFQoRhcVH5DHUt",
        },
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUC",
            "label": "stableTargetId",
            "end": "bFQoRhcVH5DHUv",
        },
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUu",
            "label": "stableTargetId",
            "end": "bFQoRhcVH5DHUv",
        },
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUE",
            "label": "unitOf",
            "end": "bFQoRhcVH5DHUv",
        },
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUw",
            "label": "unitOf",
            "end": "bFQoRhcVH5DHUv",
        },
        {
            "position": 2,
            "start": "bFQoRhcVH5DHUG",
            "label": "contact",
            "end": "bFQoRhcVH5DHUx",
        },
        {
            "position": 0,
            "start": "AdditiveOrganizationalUnit",
            "label": "parentUnit",
            "end": "bFQoRhcVH5DHUx",
        },
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUE",
            "label": "parentUnit",
            "end": "bFQoRhcVH5DHUx",
        },
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUG",
            "label": "responsibleUnit",
            "end": "bFQoRhcVH5DHUx",
        },
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUw",
            "label": "stableTargetId",
            "end": "bFQoRhcVH5DHUx",
        },
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUG",
            "label": "contact",
            "end": "bFQoRhcVH5DHUz",
        },
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUy",
            "label": "stableTargetId",
            "end": "bFQoRhcVH5DHUz",
        },
        {
            "rorId": [],
            "identifierInPrimarySource": "robert-koch-institute",
            "gndId": [],
            "wikidataId": [],
            "geprisId": [],
            "viafId": [],
            "isniId": [],
            "label": "ExtractedOrganization",
            "identifier": "bFQoRhcVH5DHUC",
        },
        {
            "rorId": [],
            "identifierInPrimarySource": "rki",
            "gndId": [],
            "wikidataId": [],
            "geprisId": [],
            "viafId": [],
            "isniId": [],
            "label": "ExtractedOrganization",
            "identifier": "bFQoRhcVH5DHUu",
        },
        {"label": "MergedPrimarySource", "identifier": "00000000000000"},
        {
            "identifierInPrimarySource": "mex",
            "label": "ExtractedPrimarySource",
            "identifier": "00000000000001",
        },
        {"label": "MergedOrganizationalUnit", "identifier": "bFQoRhcVH5DHU6"},
        {"label": "MergedContactPoint", "identifier": "bFQoRhcVH5DHUB"},
        {"label": "MergedOrganizationalUnit", "identifier": "bFQoRhcVH5DHUF"},
        {"label": "MergedActivity", "identifier": "bFQoRhcVH5DHUH"},
        {
            "identifierInPrimarySource": "ps-1",
            "label": "ExtractedPrimarySource",
            "identifier": "bFQoRhcVH5DHUq",
        },
        {"label": "MergedPrimarySource", "identifier": "bFQoRhcVH5DHUr"},
        {
            "identifierInPrimarySource": "ps-2",
            "label": "ExtractedPrimarySource",
            "identifier": "bFQoRhcVH5DHUs",
            "version": "Cool Version v2.13",
        },
        {"label": "MergedPrimarySource", "identifier": "bFQoRhcVH5DHUt"},
        {"label": "MergedOrganization", "identifier": "bFQoRhcVH5DHUv"},
        {"label": "MergedOrganizationalUnit", "identifier": "bFQoRhcVH5DHUx"},
        {"label": "MergedContactPoint", "identifier": "bFQoRhcVH5DHUz"},
        {"title": "Activity Homepage", "label": "Link", "url": "https://activity-1"},
        {"title": "Unit Homepage", "label": "Link", "url": "https://unit-1-7"},
        {"label": "PreventiveOrganizationalUnit"},
        {"value": "Aktivität 1", "label": "Text", "language": "de"},
        {"value": "RKI", "label": "Text", "language": "de"},
        {"value": "RKI", "label": "Text", "language": "de"},
        {
            "value": "Robert Koch Institut ist the best",
            "label": "Text",
            "language": "de",
        },
        {"value": "An active activity.", "label": "Text", "language": "en"},
        {"value": "Robert Koch Institute", "label": "Text", "language": "en"},
        {"value": "Unit 1", "label": "Text", "language": "en"},
        {"value": "Unit 1.6", "label": "Text", "language": "en"},
        {"value": "Unit 1.7", "label": "Text", "language": "en"},
        {"value": "Eng aktiv Aktivitéit.", "label": "Text"},
    ]


@pytest.mark.integration
def test_ingest_extracted_and_rule(
    client_with_api_key_write_permission: TestClient,
) -> None:
    extracted_item = ExtractedPerson(
        identifierInPrimarySource="1",
        hadPrimarySource=MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
        fullName=["Reginald Kenneth Dwight"],
    )
    rule_set = PersonRuleSetResponse(
        stableTargetId=extracted_item.stableTargetId,
        subtractive=SubtractivePerson(fullName=["Reginald Kenneth Dwight"]),
        additive=AdditivePerson(fullName=["Elton John"]),
    )
    response = client_with_api_key_write_permission.post(
        "/v0/ingest", json={"items": [extracted_item, rule_set]}
    )

    response = client_with_api_key_write_permission.get(
        "/v0/merged-item", params={"entityType": "MergedPerson"}
    )
    assert response.status_code == status.HTTP_200_OK, response.text
    assert response.json() == {
        "items": [
            {
                "affiliation": [],
                "email": [],
                "familyName": [],
                "fullName": ["Elton John"],
                "givenName": [],
                "isniId": [],
                "memberOf": [],
                "orcidId": [],
                "$type": "MergedPerson",
                "identifier": "bFQoRhcVH5DHUr",
            }
        ],
        "total": 1,
    }


def test_ingest_malformed(
    client_with_api_key_write_permission: TestClient,
) -> None:
    response = client_with_api_key_write_permission.post(
        "/v0/ingest",
        json={"items": ["FAIL!"]},
    )
    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY, response.text
    assert response.json() == {
        "detail": [
            {
                "type": "model_attributes_type",
                "loc": ["body", "items", 0],
                "msg": "Input should be a valid dictionary or object to extract fields from",
                "input": "FAIL!",
            }
        ]
    }


@pytest.mark.integration
def test_ingest_constraint_violation(
    client_with_api_key_write_permission: TestClient,
) -> None:
    # given a simple item saved successfully to the backend
    contact_point = ExtractedContactPoint(
        email="101@test.tld",
        identifierInPrimarySource="test-101",
        hadPrimarySource=MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
    )
    raw_item = contact_point.model_dump(mode="json")
    response = client_with_api_key_write_permission.post(
        "/v0/ingest", json={"items": [raw_item]}
    )
    assert response.status_code == status.HTTP_204_NO_CONTENT, response.text
    response = client_with_api_key_write_permission.get(
        "/v0/extracted-item", params={"entityType": contact_point.entityType}
    )
    assert response.json() == {
        "items": [
            {
                "hadPrimarySource": "00000000000000",
                "identifierInPrimarySource": "test-101",
                "email": ["101@test.tld"],
                "$type": "ExtractedContactPoint",
                "identifier": "bFQoRhcVH5DHUq",
                "stableTargetId": "bFQoRhcVH5DHUr",
            }
        ],
        "total": 1,
    }

    # when trying to post the same item with a different id
    raw_item["identifier"] = "thisIdIsNotTheOldId"
    response = client_with_api_key_write_permission.post(
        "/v0/ingest", json={"items": [raw_item]}
    )

    # then we expect the backend to reject the request
    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY, response.text
    assert "Cannot set computed fields to custom values!" in response.text

    # and expect the database to still contain the first version
    response = client_with_api_key_write_permission.get(
        "/v0/extracted-item", params={"entityType": contact_point.entityType}
    )
    assert response.json() == {
        "items": [
            {
                "hadPrimarySource": "00000000000000",
                "identifierInPrimarySource": "test-101",
                "email": ["101@test.tld"],
                "$type": "ExtractedContactPoint",
                "identifier": "bFQoRhcVH5DHUq",
                "stableTargetId": "bFQoRhcVH5DHUr",
            }
        ],
        "total": 1,
    }


@pytest.mark.usefixtures("mocked_graph", "mocked_valkey")
def test_ingest_mocked(
    client_with_api_key_write_permission: TestClient,
    dummy_data: dict[str, AnyExtractedModel],
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setattr(GraphConnector, "ingest_items", MagicMock())
    response = client_with_api_key_write_permission.post(
        "/v0/ingest", json={"items": list(dummy_data.values())}
    )
    assert response.status_code == status.HTTP_204_NO_CONTENT, response.text
