from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest
from pytest import MonkeyPatch
from starlette import status

from mex.backend.graph.connector import GraphConnector
from mex.common.models import (
    MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
    ActivityRuleSetResponse,
    AdditiveActivity,
    AdditivePerson,
    AnyExtractedModel,
    AnyRuleSetResponse,
    ExtractedContactPoint,
    ExtractedPerson,
    PersonRuleSetResponse,
    SubtractiveActivity,
    SubtractivePerson,
)
from mex.common.types import Text
from tests.conftest import DummyData, get_graph

if TYPE_CHECKING:  # pragma: no cover
    from fastapi.testclient import TestClient


@pytest.mark.integration
def test_ingest_empty(client_with_api_key_write_permission: TestClient) -> None:
    response = client_with_api_key_write_permission.post(
        "/v0/ingest", json={"items": []}
    )
    assert response.status_code == status.HTTP_204_NO_CONTENT, response.text
    assert response.text == ""


@pytest.mark.integration
def test_ingest_extracted(
    client_with_api_key_write_permission: TestClient, dummy_data: DummyData
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
            "start": "AdditiveOrganizationalUnit",
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
            "start": "SubtractiveOrganizationalUnit",
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
            "title": "Unit Homepage",
            "url": "https://unit-1-6",
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
            "value": "Abteilung 1.6",
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
            "language": "en",
            "value": "Unit 1.6",
        },
        {
            "label": "Text",
            "value": "Eng aktiv Aktivitéit.",
        },
    ]


@pytest.mark.integration
def test_ingest_rule_set(
    client_with_api_key_write_permission: TestClient,
    loaded_dummy_data: DummyData,
) -> None:
    # post the rule set to the ingest endpoint
    response = client_with_api_key_write_permission.post(
        "/v0/ingest",
        json={
            "items": [
                ActivityRuleSetResponse(
                    additive=AdditiveActivity(title=[Text(value="A1", language=None)]),
                    subtractive=SubtractiveActivity(
                        contact=loaded_dummy_data["contact_point_2"].stableTargetId
                    ),
                    stableTargetId=loaded_dummy_data["activity_1"].stableTargetId,
                )
            ]
        },
    )

    # assert the request was accepted
    assert response.status_code == status.HTTP_204_NO_CONTENT, response.text

    # verify the nodes have actually been stored in the database
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
            "activityType": [],
            "end": [],
            "fundingProgram": [],
            "label": "AdditiveActivity",
            "start": [],
            "theme": [],
        },
        {
            "activityType": [],
            "end": [],
            "fundingProgram": [],
            "label": "SubtractiveActivity",
            "start": [],
            "theme": [],
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
            "start": "AdditiveOrganizationalUnit",
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
            "start": "SubtractiveOrganizationalUnit",
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
            "start": "AdditiveActivity",
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
            "position": 0,
            "start": "SubtractiveActivity",
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
            "start": "AdditiveActivity",
        },
        {
            "end": "bFQoRhcVH5DHUH",
            "label": "stableTargetId",
            "position": 0,
            "start": "PreventiveActivity",
        },
        {
            "end": "bFQoRhcVH5DHUH",
            "label": "stableTargetId",
            "position": 0,
            "start": "SubtractiveActivity",
        },
        {
            "end": "bFQoRhcVH5DHUH",
            "label": "stableTargetId",
            "position": 0,
            "start": "WorkflowActivity",
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
            "label": "WorkflowActivity",
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
            "title": "Unit Homepage",
            "url": "https://unit-1-6",
        },
        {
            "label": "Link",
            "url": "https://ou-1",
        },
        {
            "label": "PreventiveActivity",
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
            "value": "Abteilung 1.6",
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
            "language": "en",
            "value": "Unit 1.6",
        },
        {
            "label": "Text",
            "value": "A1",
        },
        {
            "label": "Text",
            "value": "Eng aktiv Aktivitéit.",
        },
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
    assert response.status_code == status.HTTP_204_NO_CONTENT, response.text

    response = client_with_api_key_write_permission.get(
        "/v0/merged-item/datenkompass",
        params={"entityType": "MergedPerson"},
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
                "supersededBy": None,
            }
        ],
        "total": 1,
    }


@pytest.mark.integration
def test_ingest_artificial_data(
    client_with_api_key_write_permission: TestClient,
    artificial_data: list[AnyExtractedModel | AnyRuleSetResponse],
) -> None:
    response = client_with_api_key_write_permission.post(
        "/v0/ingest", json={"items": artificial_data}
    )
    assert response.status_code == status.HTTP_204_NO_CONTENT, response.text

    response = client_with_api_key_write_permission.get(
        "/v0/merged-item/datenkompass", params={"skip": "29", "limit": "1"}
    )
    assert response.status_code == status.HTTP_200_OK, response.text
    assert response.json() == {
        "items": [
            {
                "parentUnit": "bFQoRhcVH5DHUJ",
                "name": [
                    {
                        "value": "Sehen wollen Garten Erde Angst wirklich.",
                        "language": "de",
                    }
                ],
                "alternativeName": [],
                "email": ["info@rki.de"],
                "shortName": [],
                "unitOf": [],
                "website": [
                    {"language": None, "title": None, "url": "http://www.bohnbach.com/"}
                ],
                "$type": "MergedOrganizationalUnit",
                "identifier": "bFQoRhcVH5DHVJ",
                "supersededBy": None,
            }
        ],
        "total": 43,
    }


def test_ingest_malformed(
    client_with_api_key_write_permission: TestClient,
) -> None:
    response = client_with_api_key_write_permission.post(
        "/v0/ingest",
        json={"items": ["FAIL!"]},
    )
    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT, response.text
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
    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT, response.text
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
    dummy_data: DummyData,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setattr(GraphConnector, "ingest_items", MagicMock())
    response = client_with_api_key_write_permission.post(
        "/v0/ingest", json={"items": list(dummy_data.values())}
    )
    assert response.status_code == status.HTTP_204_NO_CONTENT, response.text
