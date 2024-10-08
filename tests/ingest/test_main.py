from collections import defaultdict
from typing import Any, cast

import pytest
from fastapi.testclient import TestClient

from mex.backend.graph.connector import GraphConnector
from mex.common.models import AnyExtractedModel
from tests.conftest import MockedGraph

Payload = dict[str, list[dict[str, Any]]]


@pytest.fixture()
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
    assert response.status_code == 201, response.text
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
    assert response.status_code == 201, response.text
    assert sorted(response.json()["identifiers"]) == expected_identifiers

    # verify the nodes have actually been stored in the database
    graph = GraphConnector.get()
    result = graph.fetch_extracted_items(None, None, None, 1, len(dummy_data))
    assert [i["identifier"] for i in result["items"]] == expected_identifiers


def test_bulk_insert_malformed(
    client_with_api_key_write_permission: TestClient,
) -> None:
    expected_res = []
    exp_err = {
        "ctx": {"error": {}},
        "input": "FAIL!",
        "loc": ["body", "items", 0, "function-wrap[fix_listyness()]"],
        "msg": "Assertion failed, Input should be a valid dictionary, validating "
        "other types is not supported for models with computed fields.",
        "type": "assertion_error",
    }
    expected_res += [exp_err] * 11

    response = client_with_api_key_write_permission.post(
        "/v0/ingest",
        json={"items": ["FAIL!"]},
    )
    assert response.status_code == 422, response.text
    assert response.json() == {"detail": expected_res}


def test_bulk_insert_mocked(
    client_with_api_key_write_permission: TestClient,
    post_payload: Payload,
    dummy_data: dict[str, AnyExtractedModel],
    mocked_graph: MockedGraph,
) -> None:
    mocked_graph.return_value = []
    response = client_with_api_key_write_permission.post(
        "/v0/ingest", json=post_payload
    )
    assert response.status_code == 201, response.text
    assert sorted(response.json()["identifiers"]) == sorted(
        d.identifier for d in dummy_data.values()
    )
