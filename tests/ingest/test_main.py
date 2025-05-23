from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient
from pytest import MonkeyPatch
from starlette import status

from mex.backend.graph.connector import GraphConnector
from mex.common.models import (
    EXTRACTED_MODEL_CLASSES,
    RULE_SET_RESPONSE_CLASSES,
    AnyExtractedModel,
)
from tests.conftest import get_graph


@pytest.mark.integration
def test_bulk_insert_empty(client_with_api_key_write_permission: TestClient) -> None:
    response = client_with_api_key_write_permission.post(
        "/v0/ingest", json={"items": []}
    )
    assert response.status_code == status.HTTP_204_NO_CONTENT, response.text
    assert response.text == ""


@pytest.mark.integration
def test_bulk_insert(
    client_with_api_key_write_permission: TestClient,
    dummy_data: dict[str, AnyExtractedModel],
) -> None:
    # post the artificial data to the ingest endpoint
    response = client_with_api_key_write_permission.post(
        "/v0/ingest", json={"items": list(dummy_data.values())}
    )

    # assert the response are the artificial data items
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
        {"value": "Une activité active.", "label": "Text"},
    ]


def test_bulk_insert_malformed(
    client_with_api_key_write_permission: TestClient,
) -> None:
    expected_response = []
    exp_err = {
        "ctx": {"error": {}},
        "input": "FAIL!",
        "loc": ["body", "items", 0, "function-wrap[fix_listyness()]"],
        "msg": "Assertion failed, Input should be a valid dictionary, "
        "validating other types is not supported for models with "
        "computed fields.",
        "type": "assertion_error",
    }
    expected_response += [exp_err] * len(EXTRACTED_MODEL_CLASSES)
    exp_err = {
        "input": "FAIL!",
        "loc": ["body", "items", 0, "function-wrap[fix_listyness()]"],
        "msg": "Input should be a valid dictionary or object to extract fields from",
        "type": "model_attributes_type",
    }
    expected_response += [exp_err] * len(RULE_SET_RESPONSE_CLASSES)

    response = client_with_api_key_write_permission.post(
        "/v0/ingest",
        json={"items": ["FAIL!"]},
    )
    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY, response.text
    assert response.json() == {"detail": expected_response}


@pytest.mark.usefixtures("mocked_graph", "mocked_redis")
def test_bulk_insert_mocked(
    client_with_api_key_write_permission: TestClient,
    dummy_data: dict[str, AnyExtractedModel],
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setattr(GraphConnector, "ingest_v2", MagicMock())
    response = client_with_api_key_write_permission.post(
        "/v0/ingest", json={"items": list(dummy_data.values())}
    )
    assert response.status_code == status.HTTP_204_NO_CONTENT, response.text
