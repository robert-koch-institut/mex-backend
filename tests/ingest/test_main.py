from collections import defaultdict
from typing import Any, cast
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient
from pytest import MonkeyPatch
from starlette import status

from mex.backend.graph.connector import GraphConnector
from mex.common.models import EXTRACTED_MODEL_CLASSES, AnyExtractedModel

Payload = dict[str, list[dict[str, Any]]]


@pytest.fixture
def post_payload(dummy_data: dict[str, AnyExtractedModel]) -> Payload:
    payload = defaultdict(list)
    for model in dummy_data.values():
        payload["items"].append(model.model_dump())
    return cast(Payload, dict(payload))


@pytest.mark.integration
def test_bulk_insert_empty(client_with_api_key_write_permission: TestClient) -> None:
    response = client_with_api_key_write_permission.post(
        "/v0/ingest", json={"items": []}
    )
    assert response.status_code == status.HTTP_201_CREATED, response.text
    assert response.json() == {"identifiers": []}


@pytest.mark.integration
def test_bulk_insert(
    client_with_api_key_write_permission: TestClient,
    post_payload: Payload,
    dummy_data: dict[str, AnyExtractedModel],
) -> None:
    # get expected identifiers from the dummy data
    expected_identifiers = sorted(d.identifier for d in dummy_data.values())

    # post the dummy data to the ingest endpoint
    response = client_with_api_key_write_permission.post(
        "/v0/ingest", json=post_payload
    )

    # assert the response is the identifier of the contact point
    assert response.status_code == status.HTTP_201_CREATED, response.text
    assert sorted(response.json()["identifiers"]) == expected_identifiers

    # verify the nodes have actually been stored in the database
    graph = GraphConnector.get()
    result = graph.fetch_extracted_items(None, None, None, 1, len(dummy_data))
    assert [i["identifier"] for i in result["items"]] == expected_identifiers


def test_bulk_insert_malformed(
    client_with_api_key_write_permission: TestClient,
) -> None:
    expected_response = []
    exp_err = {
        "ctx": {"error": {}},
        "input": "FAIL!",
        "loc": ["body", "items", 0, "function-wrap[fix_listyness()]"],
        "msg": "Assertion failed, Input should be a valid dictionary, validating "
        "other types is not supported for models with computed fields.",
        "type": "assertion_error",
    }
    expected_response += [exp_err] * len(EXTRACTED_MODEL_CLASSES)

    response = client_with_api_key_write_permission.post(
        "/v0/ingest",
        json={"items": ["FAIL!"]},
    )
    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY, response.text
    assert response.json() == {"detail": expected_response}


@pytest.mark.usefixtures("mocked_graph")
def test_bulk_insert_mocked(
    client_with_api_key_write_permission: TestClient,
    post_payload: Payload,
    dummy_data: dict[str, AnyExtractedModel],
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setattr(GraphConnector, "_merge_item", MagicMock())
    monkeypatch.setattr(GraphConnector, "_merge_edges", MagicMock())
    response = client_with_api_key_write_permission.post(
        "/v0/ingest", json=post_payload
    )
    assert response.status_code == status.HTTP_201_CREATED, response.text
    assert sorted(response.json()["identifiers"]) == sorted(
        d.identifier for d in dummy_data.values()
    )
