from collections import defaultdict
from typing import Any, cast
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient
from pytest import MonkeyPatch
from starlette import status

from mex.backend.graph.connector import GraphConnector
from mex.backend.merged.helpers import search_merged_items_in_graph
from mex.common.models import (
    EXTRACTED_MODEL_CLASSES,
    RULE_SET_RESPONSE_CLASSES,
    AnyExtractedModel,
    PaginatedItemsContainer,
)

Payload = dict[str, list[dict[str, Any]]]


@pytest.fixture
def post_payload(artificial_extracted_items: list[AnyExtractedModel]) -> Payload:
    payload = defaultdict(list)
    for model in artificial_extracted_items:
        payload["items"].append(model.model_dump())
    return cast("Payload", dict(payload))


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
    post_payload: Payload,
    artificial_extracted_items: list[AnyExtractedModel],
) -> None:
    # post the artificial data to the ingest endpoint
    response = client_with_api_key_write_permission.post(
        "/v0/ingest", json=post_payload
    )

    # assert the response are the artificial data items
    assert response.status_code == status.HTTP_204_NO_CONTENT, response.text
    assert response.text == ""

    # verify the nodes have actually been stored in the database
    connector = GraphConnector.get()
    result = connector.fetch_extracted_items(
        None, None, None, None, 1, len(artificial_extracted_items)
    )
    result_container = PaginatedItemsContainer[AnyExtractedModel].model_validate(
        result.one()
    )
    assert set(result_container.items) == set(artificial_extracted_items)

    # verify that the merging worked correctly
    result_merged = search_merged_items_in_graph(
        None, None, None, None, 1, len(artificial_extracted_items)
    )

    assert (
        {merged_item.identifier for merged_item in result_merged.items}
        == {
            artifical_item.stableTargetId
            for artifical_item in artificial_extracted_items
        }
    )  # compare length (indirectly) and if IDs of extracted & merged items are identical


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


@pytest.mark.usefixtures("mocked_graph")
def test_bulk_insert_mocked(
    client_with_api_key_write_permission: TestClient,
    post_payload: Payload,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setattr(GraphConnector, "_merge_item", MagicMock())
    monkeypatch.setattr(GraphConnector, "_merge_edges", MagicMock())
    response = client_with_api_key_write_permission.post(
        "/v0/ingest", json=post_payload
    )
    assert response.status_code == status.HTTP_204_NO_CONTENT, response.text
