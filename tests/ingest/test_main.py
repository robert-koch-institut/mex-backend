from typing import Any

import pytest
from fastapi.testclient import TestClient

from mex.common.models import ExtractedContactPoint, ExtractedPrimarySource
from mex.common.types import Identifier


@pytest.fixture
def post_payload() -> dict[str, Any]:
    primary_source = ExtractedPrimarySource(
        title="database",
        identifierInPrimarySource="ps-1",
        hadPrimarySource=Identifier.generate(),
    )
    contact_point = ExtractedContactPoint(
        email="info@rki.de",
        identifierInPrimarySource="cp-1",
        hadPrimarySource=primary_source.stableTargetId,
    )
    return {
        "ExtractedPrimarySource": [primary_source.model_dump()],
        "ExtractedContactPoint": [contact_point.model_dump()],
    }


@pytest.mark.integration
def test_bulk_insert_empty(client_with_write_permission: TestClient) -> None:
    response = client_with_write_permission.post("/v0/ingest", json={})

    assert response.status_code == 201, response.text
    assert response.json() == {"identifiers": []}


@pytest.mark.integration
def test_bulk_insert(
    client_with_write_permission: TestClient, post_payload: dict[str, Any]
) -> None:
    # post a single contact point to ingest endpoint
    identifier = post_payload["ExtractedContactPoint"][0]["identifier"]
    stable_target_id = post_payload["ExtractedContactPoint"][0]["stableTargetId"]
    had_primary_source = post_payload["ExtractedContactPoint"][0]["hadPrimarySource"]
    primary_source_id = post_payload["ExtractedPrimarySource"][0]["identifier"]
    response = client_with_write_permission.post("/v0/ingest", json=post_payload)

    # assert the response is the identifier of the contact point
    assert response.status_code == 201, response.text
    assert response.json() == {"identifiers": [str(identifier), str(primary_source_id)]}

    # verify the node has actually been stored in the backend
    response = client_with_write_permission.get(
        f"/v0/extracted-item?stableTargetId={stable_target_id}",
    )
    assert response.status_code == 200, response.text
    assert response.json() == {
        "total": 1,
        "items": [
            {
                "$type": "ContactPoint",
                "email": ["info@rki.de"],
                "identifier": str(identifier),
                "identifierInPrimarySource": "cp-1",
                "stableTargetId": str(stable_target_id),
                "hadPrimarySource": str(had_primary_source),
            }
        ],
    }


def test_bulk_insert_malformed(client_with_write_permission: TestClient) -> None:
    response = client_with_write_permission.post(
        "/v0/ingest",
        json={"ExtractedContactPoint": "FAIL!"},
    )
    assert response.status_code == 422, response.text
    assert response.json() == {
        "detail": [
            {
                "input": "FAIL!",
                "loc": ["body", "ExtractedContactPoint", 0],
                "msg": "Input should be a valid dictionary or object to extract "
                "fields from",
                "type": "model_attributes_type",
                "url": "https://errors.pydantic.dev/2.5/v/model_attributes_type",
            }
        ]
    }


@pytest.mark.usefixtures("mocked_graph")
def test_bulk_insert_mock(
    client_with_write_permission: TestClient, post_payload: dict[str, Any]
) -> None:
    response = client_with_write_permission.post("/v0/ingest", json=post_payload)
    assert response.status_code == 201, response.text
    assert response.json() == {
        "identifiers": [
            post_payload["ExtractedContactPoint"][0]["identifier"],
            post_payload["ExtractedPrimarySource"][0]["identifier"],
        ]
    }
