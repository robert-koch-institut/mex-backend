from typing import TYPE_CHECKING, Any

import pytest
from starlette import status

from tests.conftest import DummyData, DummyDataName, get_graph

if TYPE_CHECKING:  # pragma: no cover
    from fastapi.testclient import TestClient


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
        "workflow": {
            "$type": "WorkflowActivity",
            "forbiddenPublishingTarget": [],
        },
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
        {
            "end": "bFQoRhcVH5DHUq",
            "label": "stableTargetId",
            "position": 0,
            "start": "WorkflowActivity",
        },
        {
            "forbiddenPublishingTarget": [],
            "label": "WorkflowActivity",
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


@pytest.mark.parametrize(
    ("rule_name", "expected"),
    [
        pytest.param(
            "unit_2_rule_set",
            {
                "$type": "OrganizationalUnitRuleSetResponse",
                "additive": {
                    "$type": "AdditiveOrganizationalUnit",
                    "alternativeName": [],
                    "email": [],
                    "name": [{"language": "de", "value": "Abteilung 1.6"}],
                    "parentUnit": "bFQoRhcVH5DHUx",
                    "shortName": [],
                    "supersededBy": None,
                    "unitOf": [],
                    "website": [
                        {
                            "language": None,
                            "title": "Unit Homepage",
                            "url": "https://unit-1-6",
                        }
                    ],
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
                "stableTargetId": "bFQoRhcVH5DHUz",
                "subtractive": {
                    "$type": "SubtractiveOrganizationalUnit",
                    "alternativeName": [],
                    "email": [],
                    "name": [{"language": "en", "value": "Unit 1.6"}],
                    "parentUnit": [],
                    "shortName": [],
                    "unitOf": [],
                    "website": [],
                },
                "workflow": {
                    "$type": "WorkflowOrganizationalUnit",
                    "forbiddenPublishingTarget": [],
                },
            },
            id="rule-set-with-extracted",
        ),
        pytest.param(
            "unit_3_standalone_rule_set",
            {
                "$type": "OrganizationalUnitRuleSetResponse",
                "additive": {
                    "$type": "AdditiveOrganizationalUnit",
                    "alternativeName": [],
                    "email": ["1.7@rki.de"],
                    "name": [{"language": "de", "value": "Abteilung 1.7"}],
                    "parentUnit": "bFQoRhcVH5DHUx",
                    "shortName": [],
                    "supersededBy": None,
                    "unitOf": [],
                    "website": [],
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
                "stableTargetId": "StandaloneRule",
                "subtractive": {
                    "$type": "SubtractiveOrganizationalUnit",
                    "alternativeName": [],
                    "email": [],
                    "name": [],
                    "parentUnit": [],
                    "shortName": [],
                    "unitOf": [],
                    "website": [],
                },
                "workflow": {
                    "$type": "WorkflowOrganizationalUnit",
                    "forbiddenPublishingTarget": [],
                },
            },
            id="standalone-rule-set",
        ),
    ],
)
@pytest.mark.integration
def test_get_rule_set(
    client_with_api_key_write_permission: TestClient,
    loaded_dummy_data: DummyData,
    rule_name: DummyDataName,
    expected: dict[str, Any],
) -> None:
    response = client_with_api_key_write_permission.get(
        f"/v0/rule-set/{loaded_dummy_data[rule_name].stableTargetId}"
    )
    assert response.status_code == status.HTTP_200_OK, response.text
    assert response.json() == expected


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
            "workflow": {"$type": "WorkflowOrganizationalUnit"},
        },
    )
    assert "no merged item found" in response.text


@pytest.mark.integration
def test_delete_rule_set(
    client_with_api_key_write_permission: TestClient,
    loaded_dummy_data: DummyData,
) -> None:
    # Get rule set identifier
    identifier = loaded_dummy_data["unit_2_rule_set"].stableTargetId

    # Verify rule set exists
    response = client_with_api_key_write_permission.get(f"/v0/rule-set/{identifier}")
    assert response.status_code == status.HTTP_200_OK, response.text

    # Delete the rule set
    response = client_with_api_key_write_permission.delete(f"/v0/rule-set/{identifier}")
    assert response.status_code == status.HTTP_204_NO_CONTENT, response.text
    assert response.content == b""

    # Verify rule set is deleted (should return 404)
    response = client_with_api_key_write_permission.get(f"/v0/rule-set/{identifier}")
    assert response.status_code == status.HTTP_404_NOT_FOUND, response.text


@pytest.mark.integration
def test_delete_rule_set_without_rules(
    client_with_api_key_write_permission: TestClient,
    loaded_dummy_data: DummyData,
) -> None:
    # Get an item that has no rules
    activity_1 = loaded_dummy_data["activity_1"]

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
    loaded_dummy_data: DummyData,
) -> None:
    response = client_with_api_key_write_permission.put(
        f"/v0/rule-set/{loaded_dummy_data['unit_2_rule_set'].stableTargetId}",
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
            "workflow": {
                "$type": "WorkflowOrganizationalUnit",
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
        "workflow": {
            "$type": "WorkflowOrganizationalUnit",
            "forbiddenPublishingTarget": [],
        },
        "$type": "OrganizationalUnitRuleSetResponse",
        "stableTargetId": loaded_dummy_data["unit_2_rule_set"].stableTargetId,
    }
    assert get_graph() == [
        {
            "activityType": [],
            "end": [],
            "fundingProgram": [],
            "identifier": "bFQoRhcVH5DHUG",
            "identifierInPrimarySource": "a-1",
            "label": "ExtractedActivity",
            "start": [
                "2014-08-24",
            ],
            "theme": [
                "https://mex.rki.de/item/theme-11",
            ],
        },
        {
            "email": [
                "1.7@rki.de",
            ],
            "label": "AdditiveOrganizationalUnit",
        },
        {
            "email": [
                "help@contact-point.two",
            ],
            "identifier": "bFQoRhcVH5DHUC",
            "identifierInPrimarySource": "cp-2",
            "label": "ExtractedContactPoint",
        },
        {
            "email": [
                "info@contact-point.one",
            ],
            "identifier": "bFQoRhcVH5DHUA",
            "identifierInPrimarySource": "cp-1",
            "label": "ExtractedContactPoint",
        },
        {
            "email": [],
            "identifier": "bFQoRhcVH5DHUw",
            "identifierInPrimarySource": "ou-1",
            "label": "ExtractedOrganizationalUnit",
        },
        {
            "email": [],
            "identifier": "bFQoRhcVH5DHUy",
            "identifierInPrimarySource": "ou-1.6",
            "label": "ExtractedOrganizationalUnit",
        },
        {
            "email": [],
            "label": "AdditiveOrganizationalUnit",
        },
        {
            "email": [],
            "label": "SubtractiveOrganizationalUnit",
        },
        {
            "email": [],
            "label": "SubtractiveOrganizationalUnit",
        },
        {
            "end": "00000000000000",
            "label": "hadPrimarySource",
            "position": 0,
            "start": "00000000000001",
        },
        {
            "end": "00000000000000",
            "label": "hadPrimarySource",
            "position": 0,
            "start": "00000000000003",
        },
        {
            "end": "00000000000000",
            "label": "hadPrimarySource",
            "position": 0,
            "start": "bFQoRhcVH5DHUq",
        },
        {
            "end": "00000000000000",
            "label": "hadPrimarySource",
            "position": 0,
            "start": "bFQoRhcVH5DHUs",
        },
        {
            "end": "00000000000000",
            "label": "stableTargetId",
            "position": 0,
            "start": "00000000000001",
        },
        {
            "end": "00000000000002",
            "label": "stableTargetId",
            "position": 0,
            "start": "00000000000003",
        },
        {
            "end": "Link",
            "label": "website",
            "position": 0,
            "start": "bFQoRhcVH5DHUG",
        },
        {
            "end": "Link",
            "label": "website",
            "position": 0,
            "start": "bFQoRhcVH5DHUw",
        },
        {
            "end": "StandaloneRule",
            "label": "stableTargetId",
            "position": 0,
            "start": "AdditiveOrganizationalUnit",
        },
        {
            "end": "StandaloneRule",
            "label": "stableTargetId",
            "position": 0,
            "start": "PreventiveOrganizationalUnit",
        },
        {
            "end": "StandaloneRule",
            "label": "stableTargetId",
            "position": 0,
            "start": "SubtractiveOrganizationalUnit",
        },
        {
            "end": "StandaloneRule",
            "label": "stableTargetId",
            "position": 0,
            "start": "WorkflowOrganizationalUnit",
        },
        {
            "end": "Text",
            "label": "abstract",
            "position": 0,
            "start": "bFQoRhcVH5DHUG",
        },
        {
            "end": "Text",
            "label": "abstract",
            "position": 1,
            "start": "bFQoRhcVH5DHUG",
        },
        {
            "end": "Text",
            "label": "name",
            "position": 0,
            "start": "AdditiveOrganizationalUnit",
        },
        {
            "end": "Text",
            "label": "name",
            "position": 0,
            "start": "AdditiveOrganizationalUnit",
        },
        {
            "end": "Text",
            "label": "name",
            "position": 0,
            "start": "bFQoRhcVH5DHUw",
        },
        {
            "end": "Text",
            "label": "name",
            "position": 0,
            "start": "bFQoRhcVH5DHUy",
        },
        {
            "end": "Text",
            "label": "officialName",
            "position": 0,
            "start": "bFQoRhcVH5DHUE",
        },
        {
            "end": "Text",
            "label": "officialName",
            "position": 0,
            "start": "bFQoRhcVH5DHUu",
        },
        {
            "end": "Text",
            "label": "officialName",
            "position": 1,
            "start": "bFQoRhcVH5DHUE",
        },
        {
            "end": "Text",
            "label": "title",
            "position": 0,
            "start": "bFQoRhcVH5DHUG",
        },
        {
            "end": "bFQoRhcVH5DHUB",
            "label": "contact",
            "position": 0,
            "start": "bFQoRhcVH5DHUG",
        },
        {
            "end": "bFQoRhcVH5DHUB",
            "label": "stableTargetId",
            "position": 0,
            "start": "bFQoRhcVH5DHUA",
        },
        {
            "end": "bFQoRhcVH5DHUD",
            "label": "contact",
            "position": 1,
            "start": "bFQoRhcVH5DHUG",
        },
        {
            "end": "bFQoRhcVH5DHUD",
            "label": "stableTargetId",
            "position": 0,
            "start": "bFQoRhcVH5DHUC",
        },
        {
            "end": "bFQoRhcVH5DHUF",
            "label": "stableTargetId",
            "position": 0,
            "start": "bFQoRhcVH5DHUE",
        },
        {
            "end": "bFQoRhcVH5DHUH",
            "label": "stableTargetId",
            "position": 0,
            "start": "bFQoRhcVH5DHUG",
        },
        {
            "end": "bFQoRhcVH5DHUr",
            "label": "hadPrimarySource",
            "position": 0,
            "start": "bFQoRhcVH5DHUA",
        },
        {
            "end": "bFQoRhcVH5DHUr",
            "label": "hadPrimarySource",
            "position": 0,
            "start": "bFQoRhcVH5DHUC",
        },
        {
            "end": "bFQoRhcVH5DHUr",
            "label": "hadPrimarySource",
            "position": 0,
            "start": "bFQoRhcVH5DHUG",
        },
        {
            "end": "bFQoRhcVH5DHUr",
            "label": "hadPrimarySource",
            "position": 0,
            "start": "bFQoRhcVH5DHUu",
        },
        {
            "end": "bFQoRhcVH5DHUr",
            "label": "stableTargetId",
            "position": 0,
            "start": "bFQoRhcVH5DHUq",
        },
        {
            "end": "bFQoRhcVH5DHUt",
            "label": "hadPrimarySource",
            "position": 0,
            "start": "bFQoRhcVH5DHUE",
        },
        {
            "end": "bFQoRhcVH5DHUt",
            "label": "hadPrimarySource",
            "position": 0,
            "start": "bFQoRhcVH5DHUw",
        },
        {
            "end": "bFQoRhcVH5DHUt",
            "label": "hadPrimarySource",
            "position": 0,
            "start": "bFQoRhcVH5DHUy",
        },
        {
            "end": "bFQoRhcVH5DHUt",
            "label": "stableTargetId",
            "position": 0,
            "start": "bFQoRhcVH5DHUs",
        },
        {
            "end": "bFQoRhcVH5DHUv",
            "label": "stableTargetId",
            "position": 0,
            "start": "bFQoRhcVH5DHUu",
        },
        {
            "end": "bFQoRhcVH5DHUv",
            "label": "unitOf",
            "position": 0,
            "start": "bFQoRhcVH5DHUw",
        },
        {
            "end": "bFQoRhcVH5DHUv",
            "label": "unitOf",
            "position": 0,
            "start": "bFQoRhcVH5DHUy",
        },
        {
            "end": "bFQoRhcVH5DHUx",
            "label": "contact",
            "position": 2,
            "start": "bFQoRhcVH5DHUG",
        },
        {
            "end": "bFQoRhcVH5DHUx",
            "label": "parentUnit",
            "position": 0,
            "start": "AdditiveOrganizationalUnit",
        },
        {
            "end": "bFQoRhcVH5DHUx",
            "label": "responsibleUnit",
            "position": 0,
            "start": "bFQoRhcVH5DHUG",
        },
        {
            "end": "bFQoRhcVH5DHUx",
            "label": "stableTargetId",
            "position": 0,
            "start": "bFQoRhcVH5DHUw",
        },
        {
            "end": "bFQoRhcVH5DHUz",
            "label": "stableTargetId",
            "position": 0,
            "start": "AdditiveOrganizationalUnit",
        },
        {
            "end": "bFQoRhcVH5DHUz",
            "label": "stableTargetId",
            "position": 0,
            "start": "PreventiveOrganizationalUnit",
        },
        {
            "end": "bFQoRhcVH5DHUz",
            "label": "stableTargetId",
            "position": 0,
            "start": "SubtractiveOrganizationalUnit",
        },
        {
            "end": "bFQoRhcVH5DHUz",
            "label": "stableTargetId",
            "position": 0,
            "start": "WorkflowOrganizationalUnit",
        },
        {
            "end": "bFQoRhcVH5DHUz",
            "label": "stableTargetId",
            "position": 0,
            "start": "bFQoRhcVH5DHUy",
        },
        {
            "forbiddenPublishingTarget": [],
            "label": "WorkflowOrganizationalUnit",
        },
        {
            "forbiddenPublishingTarget": [],
            "label": "WorkflowOrganizationalUnit",
        },
        {
            "geprisId": [],
            "gndId": [],
            "identifier": "bFQoRhcVH5DHUE",
            "identifierInPrimarySource": "robert-koch-institute",
            "isniId": [],
            "label": "ExtractedOrganization",
            "rorId": [],
            "viafId": [],
            "wikidataId": [],
        },
        {
            "geprisId": [],
            "gndId": [],
            "identifier": "bFQoRhcVH5DHUu",
            "identifierInPrimarySource": "rki",
            "isniId": [],
            "label": "ExtractedOrganization",
            "rorId": [],
            "viafId": [],
            "wikidataId": [],
        },
        {
            "identifier": "00000000000000",
            "label": "MergedPrimarySource",
        },
        {
            "identifier": "00000000000001",
            "identifierInPrimarySource": "mex",
            "label": "ExtractedPrimarySource",
        },
        {
            "identifier": "00000000000002",
            "label": "MergedPrimarySource",
        },
        {
            "identifier": "00000000000003",
            "identifierInPrimarySource": "mex-editor",
            "label": "ExtractedPrimarySource",
        },
        {
            "identifier": "StandaloneRule",
            "label": "MergedOrganizationalUnit",
        },
        {
            "identifier": "bFQoRhcVH5DHUB",
            "label": "MergedContactPoint",
        },
        {
            "identifier": "bFQoRhcVH5DHUD",
            "label": "MergedContactPoint",
        },
        {
            "identifier": "bFQoRhcVH5DHUF",
            "label": "MergedOrganization",
        },
        {
            "identifier": "bFQoRhcVH5DHUH",
            "label": "MergedActivity",
        },
        {
            "identifier": "bFQoRhcVH5DHUq",
            "identifierInPrimarySource": "ps-1",
            "label": "ExtractedPrimarySource",
        },
        {
            "identifier": "bFQoRhcVH5DHUr",
            "label": "MergedPrimarySource",
        },
        {
            "identifier": "bFQoRhcVH5DHUs",
            "identifierInPrimarySource": "ps-2",
            "label": "ExtractedPrimarySource",
            "version": "Cool Version v2.13",
        },
        {
            "identifier": "bFQoRhcVH5DHUt",
            "label": "MergedPrimarySource",
        },
        {
            "identifier": "bFQoRhcVH5DHUv",
            "label": "MergedOrganization",
        },
        {
            "identifier": "bFQoRhcVH5DHUx",
            "label": "MergedOrganizationalUnit",
        },
        {
            "identifier": "bFQoRhcVH5DHUz",
            "label": "MergedOrganizationalUnit",
        },
        {
            "label": "Link",
            "title": "Activity Homepage",
            "url": "https://activity-1",
        },
        {
            "label": "Link",
            "url": "https://ou-1",
        },
        {
            "label": "PreventiveOrganizationalUnit",
        },
        {
            "label": "PreventiveOrganizationalUnit",
        },
        {
            "label": "Text",
            "language": "de",
            "value": "Abteilung 1.7",
        },
        {
            "label": "Text",
            "language": "de",
            "value": "Aktivität 1",
        },
        {
            "label": "Text",
            "language": "de",
            "value": "RKI",
        },
        {
            "label": "Text",
            "language": "de",
            "value": "RKI",
        },
        {
            "label": "Text",
            "language": "en",
            "value": "A new unit name",
        },
        {
            "label": "Text",
            "language": "en",
            "value": "An active activity.",
        },
        {
            "label": "Text",
            "language": "en",
            "value": "Robert Koch Institute",
        },
        {
            "label": "Text",
            "language": "en",
            "value": "Unit 1",
        },
        {
            "label": "Text",
            "language": "en",
            "value": "Unit 1.6",
        },
        {
            "label": "Text",
            "value": "Eng aktiv Aktivitéit.",
        },
    ]
