from typing import Any

import pytest
from fastapi.testclient import TestClient

from mex.backend.graph.connector import GraphConnector


def get_graph() -> dict[str, Any]:
    connector = GraphConnector.get()
    nodes = connector.commit(
        """
MATCH (n)
RETURN collect(n{
    .*, label: head(labels(n))
}) AS nodes;
"""
    )
    relations = connector.commit(
        """
MATCH ()-[r]->()
RETURN collect({
    label: type(r), position: r.position,
    start: coalesce(startNode(r).identifier, head(labels(startNode(r)))),
    end: coalesce(endNode(r).identifier, head(labels(endNode(r))))
}) AS relations;
"""
    )
    return {**nodes.one(), **relations.one()}


@pytest.mark.integration
def test_create_rule_set(client_with_api_key_write_permission: TestClient) -> None:
    response = client_with_api_key_write_permission.post(
        "/v0/rule-set",
        json={
            "addValues": {
                "start": ["2025"],
                "title": [{"value": "A new beginning", "language": "en"}],
            },
            "blockValues": {
                "start": ["2025-01-01"],
                "title": [{"value": "Ein alter Hut", "language": "de"}],
            },
            "blockPrimarySources": {},
        },
        # {
        #     "start": {
        #         "addValues": ["2025"],
        #         "blockValues": ["2025-01-01"],
        #         "blockPrimarySources": [],
        #     },
        #     "title": {
        #         "addValues": [{"value": "A new beginning", "language": "en"}],
        #         "blockValues": [{"value": "Ein alter Hut", "language": "de"}],
        #         "blockPrimarySources": [],
        #     },
        # },
    )
    assert response.status_code == 204, response.text

    assert get_graph() == {
        "nodes": [
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
                "identifier": "bFQoRhcVH5DHUq",
                "label": "MergedActivity",
            },
            {
                "fundingProgram": [],
                "start": ["2025"],
                "end": [],
                "theme": [],
                "label": "AdditiveActivity",
                "activityType": [],
            },
            {
                "language": "en",
                "label": "Text",
                "value": "A new beginning",
            },
            {
                "fundingProgram": [],
                "start": ["2025-01-01"],
                "end": [],
                "theme": [],
                "label": "SubtractiveActivity",
                "activityType": [],
            },
            {
                "language": "de",
                "label": "Text",
                "value": "Ein alter Hut",
            },
        ],
        "relations": [
            {
                "start": "00000000000001",
                "end": "00000000000000",
                "label": "stableTargetId",
                "position": 0,
            },
            {
                "start": "00000000000001",
                "end": "00000000000000",
                "label": "hadPrimarySource",
                "position": 0,
            },
            {
                "start": "AdditiveActivity",
                "end": "bFQoRhcVH5DHUq",
                "label": "stableTargetId",
                "position": 0,
            },
            {
                "start": "AdditiveActivity",
                "end": "Text",
                "label": "title",
                "position": 0,
            },
            {
                "start": "AdditiveActivity",
                "end": "00000000000000",
                "label": "hadPrimarySource",
                "position": 0,
            },
            {
                "start": "SubtractiveActivity",
                "end": "bFQoRhcVH5DHUq",
                "label": "stableTargetId",
                "position": 0,
            },
            {
                "start": "SubtractiveActivity",
                "end": "Text",
                "label": "title",
                "position": 0,
            },
            {
                "start": "SubtractiveActivity",
                "end": "00000000000000",
                "label": "hadPrimarySource",
                "position": 0,
            },
        ],
    }
