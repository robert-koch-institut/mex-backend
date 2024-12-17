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
            "start": "00000000000001",
            "end": "00000000000000",
            "label": "stableTargetId",
            "position": 0,
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
            "parentUnit": "bFQoRhcVH5DHUv",
            "name": [{"value": "Unit 1.7", "language": "en"}],
            "alternativeName": [],
            "email": [],
            "shortName": [],
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
        "stableTargetId": "bFQoRhcVH5DHUB",
    }


@pytest.mark.integration
def test_update_rule_set_not_found(
    client_with_api_key_write_permission: TestClient,
) -> None:
    response = client_with_api_key_write_permission.put(
        "/v0/rule-set/thisIdDoesNotExist",
        json={
            "additive": {"$type": "AdditiveOrganizationalUnit"},
            "preventive": {"$type": "PreventiveOrganizationalUnit"},
            "subtractive": {"$type": "SubtractiveOrganizationalUnit"},
        },
    )
    assert "no merged item found" in response.text


@pytest.mark.integration
def test_update_rule_set(
    client_with_api_key_write_permission: TestClient,
    load_dummy_rule_set: OrganizationalUnitRuleSetResponse,
) -> None:
    response = client_with_api_key_write_permission.put(
        f"/v0/rule-set/{load_dummy_rule_set.stableTargetId}",
        json={
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
        "stableTargetId": "bFQoRhcVH5DHUB",
    }
    assert get_graph() == [
        {
            "identifierInPrimarySource": "ou-1.6",
            "email": [],
            "label": "ExtractedOrganizationalUnit",
            "identifier": "bFQoRhcVH5DHUA",
        },
        {
            "identifierInPrimarySource": "ou-1",
            "email": [],
            "label": "ExtractedOrganizationalUnit",
            "identifier": "bFQoRhcVH5DHUu",
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
            "label": "name",
            "end": "Text",
        },
        {"position": 0, "start": "bFQoRhcVH5DHUA", "label": "name", "end": "Text"},
        {"position": 0, "start": "bFQoRhcVH5DHUu", "label": "name", "end": "Text"},
        {
            "position": 0,
            "start": "AdditiveOrganizationalUnit",
            "label": "stableTargetId",
            "end": "bFQoRhcVH5DHUB",
        },
        {
            "position": 0,
            "start": "PreventiveOrganizationalUnit",
            "label": "stableTargetId",
            "end": "bFQoRhcVH5DHUB",
        },
        {
            "position": 0,
            "start": "SubtractiveOrganizationalUnit",
            "label": "stableTargetId",
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
            "start": "bFQoRhcVH5DHUA",
            "label": "hadPrimarySource",
            "end": "bFQoRhcVH5DHUt",
        },
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUu",
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
            "start": "bFQoRhcVH5DHUA",
            "label": "parentUnit",
            "end": "bFQoRhcVH5DHUv",
        },
        {
            "position": 0,
            "start": "bFQoRhcVH5DHUu",
            "label": "stableTargetId",
            "end": "bFQoRhcVH5DHUv",
        },
        {"label": "MergedPrimarySource", "identifier": "00000000000000"},
        {
            "identifierInPrimarySource": "mex",
            "label": "ExtractedPrimarySource",
            "identifier": "00000000000001",
        },
        {"label": "MergedOrganizationalUnit", "identifier": "bFQoRhcVH5DHUB"},
        {
            "identifierInPrimarySource": "ps-2",
            "label": "ExtractedPrimarySource",
            "identifier": "bFQoRhcVH5DHUs",
            "version": "Cool Version v2.13",
        },
        {"label": "MergedPrimarySource", "identifier": "bFQoRhcVH5DHUt"},
        {"label": "MergedOrganizationalUnit", "identifier": "bFQoRhcVH5DHUv"},
        {"label": "PreventiveOrganizationalUnit"},
        {"value": "A new unit name", "label": "Text", "language": "en"},
        {"value": "Unit 1", "label": "Text", "language": "en"},
        {"value": "Unit 1.6", "label": "Text", "language": "en"},
    ]
