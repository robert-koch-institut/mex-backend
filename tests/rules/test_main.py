from typing import Any

import pytest
from fastapi.testclient import TestClient
from starlette import status

from mex.common.models import OrganizationalUnitRuleSetResponse
from tests.conftest import get_graph


@pytest.mark.integration
def test_create_rule_set(client_with_api_key_write_permission: TestClient) -> None:
    response = client_with_api_key_write_permission.post(
        "/v0/rule-set",
        json={
            "$type": "ActivityRuleSetRequest",
            "additive": {
                "$type": "AdditiveActivity",
                "start": ["2025"],
                "title": [{"value": "A new beginning", "language": "en"}],
            },
            "preventive": {
                "$type": "PreventiveActivity",
                "fundingProgram": ["00000000000000"],
            },
            "subtractive": {
                "$type": "SubtractiveActivity",
                "website": [{"url": "https://activity.rule/one"}],
            },
        },
    )
    assert response.status_code == status.HTTP_201_CREATED, response.text
    assert response.json() == {
        "additive": {
            "contact": [],
            "responsibleUnit": [],
            "title": [{"value": "A new beginning", "language": "en"}],
            "abstract": [],
            "activityType": [],
            "alternativeTitle": [],
            "documentation": [],
            "end": [],
            "externalAssociate": [],
            "funderOrCommissioner": [],
            "fundingProgram": [],
            "involvedPerson": [],
            "involvedUnit": [],
            "isPartOfActivity": [],
            "publication": [],
            "shortName": [],
            "start": ["2025"],
            "succeeds": [],
            "supersededBy": None,
            "theme": [],
            "website": [],
            "$type": "AdditiveActivity",
        },
        "subtractive": {
            "contact": [],
            "responsibleUnit": [],
            "title": [],
            "abstract": [],
            "activityType": [],
            "alternativeTitle": [],
            "documentation": [],
            "end": [],
            "externalAssociate": [],
            "funderOrCommissioner": [],
            "fundingProgram": [],
            "involvedPerson": [],
            "involvedUnit": [],
            "isPartOfActivity": [],
            "publication": [],
            "shortName": [],
            "start": [],
            "succeeds": [],
            "theme": [],
            "website": [
                {"language": None, "title": None, "url": "https://activity.rule/one"}
            ],
            "$type": "SubtractiveActivity",
        },
        "preventive": {
            "$type": "PreventiveActivity",
            "abstract": [],
            "activityType": [],
            "alternativeTitle": [],
            "contact": [],
            "documentation": [],
            "end": [],
            "externalAssociate": [],
            "funderOrCommissioner": [],
            "fundingProgram": ["00000000000000"],
            "involvedPerson": [],
            "involvedUnit": [],
            "isPartOfActivity": [],
            "publication": [],
            "responsibleUnit": [],
            "shortName": [],
            "start": [],
            "succeeds": [],
            "theme": [],
            "title": [],
            "website": [],
        },
        "$type": "ActivityRuleSetResponse",
        "stableTargetId": "bFQoRhcVH5DHUq",
    }

    assert get_graph() == [
        {
            "fundingProgram": [],
            "start": ["2025"],
            "end": [],
            "theme": [],
            "label": "AdditiveActivity",
            "activityType": [],
        },
        {
            "fundingProgram": [],
            "start": [],
            "end": [],
            "theme": [],
            "label": "SubtractiveActivity",
            "activityType": [],
        },
        {
            "start": "PreventiveActivity",
            "end": "00000000000000",
            "label": "fundingProgram",
            "position": 0,
        },
        {
            "start": "00000000000001",
            "end": "00000000000000",
            "label": "hadPrimarySource",
            "position": 0,
        },
        {
            "end": "00000000000000",
            "label": "hadPrimarySource",
            "position": 0,
            "start": "00000000000003",
        },
        {
            "start": "00000000000001",
            "end": "00000000000000",
            "label": "stableTargetId",
            "position": 0,
        },
        {
            "end": "00000000000002",
            "label": "stableTargetId",
            "position": 0,
            "start": "00000000000003",
        },
        {
            "start": "SubtractiveActivity",
            "end": "Link",
            "label": "website",
            "position": 0,
        },
        {"start": "AdditiveActivity", "end": "Text", "label": "title", "position": 0},
        {
            "start": "AdditiveActivity",
            "end": "bFQoRhcVH5DHUq",
            "label": "stableTargetId",
            "position": 0,
        },
        {
            "start": "PreventiveActivity",
            "end": "bFQoRhcVH5DHUq",
            "label": "stableTargetId",
            "position": 0,
        },
        {
            "start": "SubtractiveActivity",
            "end": "bFQoRhcVH5DHUq",
            "label": "stableTargetId",
            "position": 0,
        },
        {"identifier": "00000000000000", "label": "MergedPrimarySource"},
        {
            "identifier": "00000000000001",
            "identifierInPrimarySource": "mex",
            "label": "ExtractedPrimarySource",
        },
        {"identifier": "00000000000002", "label": "MergedPrimarySource"},
        {
            "identifier": "00000000000003",
            "identifierInPrimarySource": "mex-editor",
            "label": "ExtractedPrimarySource",
        },
        {"identifier": "bFQoRhcVH5DHUq", "label": "MergedActivity"},
        {"label": "Link", "url": "https://activity.rule/one"},
        {"label": "PreventiveActivity"},
        {"language": "en", "label": "Text", "value": "A new beginning"},
    ]


@pytest.mark.integration
def test_get_rule_set_not_found(
    client_with_api_key_write_permission: TestClient,
) -> None:
    response = client_with_api_key_write_permission.get(
        "/v0/rule-set/thisIdDoesNotExist"
    )
    assert response.status_code == status.HTTP_404_NOT_FOUND, response.text
    assert response.json() == {"detail": "no rules found"}


@pytest.mark.integration
def test_get_rule_set(
    client_with_api_key_write_permission: TestClient,
    load_dummy_rule_set: OrganizationalUnitRuleSetResponse,
) -> None:
    response = client_with_api_key_write_permission.get(
        f"/v0/rule-set/{load_dummy_rule_set.stableTargetId}"
    )
    assert response.status_code == status.HTTP_200_OK, response.text
    assert response.json() == {
        "additive": {
            "parentUnit": "bFQoRhcVH5DHUx",
            "name": [{"value": "Unit 1.7", "language": "en"}],
            "alternativeName": [],
            "email": [],
            "shortName": [],
            "supersededBy": None,
            "unitOf": [],
            "website": [
                {"language": None, "title": "Unit Homepage", "url": "https://unit-1-7"}
            ],
            "$type": "AdditiveOrganizationalUnit",
        },
        "subtractive": {
            "parentUnit": [],
            "name": [],
            "alternativeName": [],
            "email": [],
            "shortName": [],
            "unitOf": [],
            "website": [],
            "$type": "SubtractiveOrganizationalUnit",
        },
        "preventive": {
            "$type": "PreventiveOrganizationalUnit",
            "alternativeName": [],
            "email": [],
            "name": [],
            "parentUnit": [],
            "shortName": [],
            "unitOf": [],
            "website": [],
        },
        "$type": "OrganizationalUnitRuleSetResponse",
        "stableTargetId": load_dummy_rule_set.stableTargetId,
    }


@pytest.mark.integration
def test_update_rule_set_not_found(
    client_with_api_key_write_permission: TestClient,
) -> None:
    response = client_with_api_key_write_permission.put(
        "/v0/rule-set/thisIdDoesNotExist",
        json={
            "$type": "OrganizationalUnitRuleSetRequest",
            "additive": {"$type": "AdditiveOrganizationalUnit"},
            "preventive": {"$type": "PreventiveOrganizationalUnit"},
            "subtractive": {"$type": "SubtractiveOrganizationalUnit"},
        },
    )
    assert "no merged item found" in response.text


@pytest.mark.integration
def test_delete_rule_set(
    client_with_api_key_write_permission: TestClient,
    load_dummy_rule_set: OrganizationalUnitRuleSetResponse,
) -> None:
    # Verify rule set exists
    response = client_with_api_key_write_permission.get(
        f"/v0/rule-set/{load_dummy_rule_set.stableTargetId}"
    )
    assert response.status_code == status.HTTP_200_OK, response.text

    # Delete the rule set
    response = client_with_api_key_write_permission.delete(
        f"/v0/rule-set/{load_dummy_rule_set.stableTargetId}"
    )
    assert response.status_code == status.HTTP_204_NO_CONTENT, response.text
    assert response.content == b""

    # Verify rule set is deleted (should return 404)
    response = client_with_api_key_write_permission.get(
        f"/v0/rule-set/{load_dummy_rule_set.stableTargetId}"
    )
    assert response.status_code == status.HTTP_404_NOT_FOUND, response.text


@pytest.mark.integration
def test_delete_rule_set_without_rules(
    client_with_api_key_write_permission: TestClient,
    load_dummy_data: dict[str, Any],
) -> None:
    # Get an item that has no rules
    activity_1 = load_dummy_data["activity_1"]

    # Delete the rule set (should succeed with 204 even though no rules exist)
    response = client_with_api_key_write_permission.delete(
        f"/v0/rule-set/{activity_1.stableTargetId}"
    )
    assert response.status_code == status.HTTP_204_NO_CONTENT, response.text
    assert response.content == b""


@pytest.mark.integration
def test_delete_rule_set_not_found(
    client_with_api_key_write_permission: TestClient,
) -> None:
    # Try to delete a rule set for a non-existent merged item
    response = client_with_api_key_write_permission.delete(
        "/v0/rule-set/thisIdDoesNotExist"
    )
    assert response.status_code == status.HTTP_404_NOT_FOUND, response.text


@pytest.mark.integration
def test_update_rule_set(
    client_with_api_key_write_permission: TestClient,
    load_dummy_rule_set: OrganizationalUnitRuleSetResponse,
) -> None:
    response = client_with_api_key_write_permission.put(
        f"/v0/rule-set/{load_dummy_rule_set.stableTargetId}",
        json={
            "$type": "OrganizationalUnitRuleSetRequest",
            "additive": {
                "$type": "AdditiveOrganizationalUnit",
                "name": [{"value": "A new unit name", "language": "en"}],
                "website": [],
            },
            "preventive": {
                "$type": "PreventiveOrganizationalUnit",
            },
            "subtractive": {
                "$type": "SubtractiveOrganizationalUnit",
            },
        },
    )
    assert response.status_code == status.HTTP_200_OK, response.text
    assert response.json() == {
        "additive": {
            "parentUnit": None,
            "name": [{"value": "A new unit name", "language": "en"}],
            "alternativeName": [],
            "email": [],
            "shortName": [],
            "supersededBy": None,
            "unitOf": [],
            "website": [],
            "$type": "AdditiveOrganizationalUnit",
        },
        "subtractive": {
            "parentUnit": [],
            "name": [],
            "alternativeName": [],
            "email": [],
            "shortName": [],
            "unitOf": [],
            "website": [],
            "$type": "SubtractiveOrganizationalUnit",
        },
        "preventive": {
            "$type": "PreventiveOrganizationalUnit",
            "alternativeName": [],
            "email": [],
            "name": [],
            "parentUnit": [],
            "shortName": [],
            "unitOf": [],
            "website": [],
        },
        "$type": "OrganizationalUnitRuleSetResponse",
        "stableTargetId": load_dummy_rule_set.stableTargetId,
    }
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
            "end": "00000000000000",
            "label": "hadPrimarySource",
            "position": 0,
            "start": "00000000000003",
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
            "end": "00000000000002",
            "label": "stableTargetId",
            "position": 0,
            "start": "00000000000003",
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
            "start": "AdditiveOrganizationalUnit",
            "label": "stableTargetId",
            "end": "bFQoRhcVH5DHUF",
        },
        {
            "position": 0,
            "start": "PreventiveOrganizationalUnit",
            "label": "stableTargetId",
            "end": "bFQoRhcVH5DHUF",
        },
        {
            "position": 0,
            "start": "SubtractiveOrganizationalUnit",
            "label": "stableTargetId",
            "end": "bFQoRhcVH5DHUF",
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
        {"identifier": "00000000000002", "label": "MergedPrimarySource"},
        {
            "identifier": "00000000000003",
            "identifierInPrimarySource": "mex-editor",
            "label": "ExtractedPrimarySource",
        },
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
        {"label": "PreventiveOrganizationalUnit"},
        {"value": "Aktivität 1", "label": "Text", "language": "de"},
        {"value": "RKI", "label": "Text", "language": "de"},
        {"value": "RKI", "label": "Text", "language": "de"},
        {
            "value": "Robert Koch Institut ist the best",
            "label": "Text",
            "language": "de",
        },
        {"value": "A new unit name", "label": "Text", "language": "en"},
        {"value": "An active activity.", "label": "Text", "language": "en"},
        {"value": "Robert Koch Institute", "label": "Text", "language": "en"},
        {"value": "Unit 1", "label": "Text", "language": "en"},
        {"value": "Unit 1.6", "label": "Text", "language": "en"},
        {"value": "Eng aktiv Aktivitéit.", "label": "Text"},
    ]
