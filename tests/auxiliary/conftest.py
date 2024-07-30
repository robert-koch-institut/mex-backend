import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, Mock

import pytest
import requests
from pytest import MonkeyPatch
from requests import Response

from mex.common.wikidata.connector import (
    WikidataAPIConnector,
    WikidataQueryServiceConnector,
)
from mex.common.wikidata.models.organization import WikidataOrganization

TEST_DATA_DIR = Path(__file__).parent / "test_data"


@pytest.fixture()
def wikidata_organization_raw() -> dict[str, Any]:
    """Return a raw wikidata organization."""
    with open(TEST_DATA_DIR / "wikidata_organization_raw.json") as fh:
        return json.load(fh)


@pytest.fixture()
def wikidata_organization(
    wikidata_organization_raw: dict[str, Any],
) -> WikidataOrganization:
    """Return a wikidata organization instance."""
    return WikidataOrganization.model_validate(wikidata_organization_raw)


@pytest.fixture()
def mocked_wikidata(
    monkeypatch: MonkeyPatch, wikidata_organization_raw: dict[str, Any]
) -> None:
    """Mock wikidata connector."""
    response_query = Mock(spec=Response, status_code=200)

    session = MagicMock(spec=requests.Session)
    session.get = MagicMock(side_effect=[response_query])

    def mocked_init(self: WikidataQueryServiceConnector) -> None:
        self.session = session

    monkeypatch.setattr(WikidataQueryServiceConnector, "__init__", mocked_init)
    monkeypatch.setattr(WikidataAPIConnector, "__init__", mocked_init)

    # mock search_wikidata_with_query

    def get_data_by_query(
        self: WikidataQueryServiceConnector, query: str
    ) -> list[dict[str, dict[str, str]]]:
        return [
            {
                "item": {
                    "type": "uri",
                    "value": "http://www.wikidata.org/entity/Q26678",
                },
                "itemLabel": {"xml:lang": "en", "type": "literal", "value": "BMW"},
                "itemDescription": {
                    "xml:lang": "en",
                    "type": "literal",
                    "value": "German automotive manufacturer, and conglomerate",
                },
                "count": {
                    "datatype": "http://www.w3.org/2001/XMLSchema#integer",
                    "type": "literal",
                    "value": "3",
                },
            },
        ]

    monkeypatch.setattr(
        WikidataQueryServiceConnector, "get_data_by_query", get_data_by_query
    )

    # mock get_wikidata_org_with_org_id

    def get_wikidata_item_details_by_id(
        self: WikidataQueryServiceConnector, item_id: str
    ) -> dict[str, str]:
        return wikidata_organization_raw

    monkeypatch.setattr(
        WikidataAPIConnector,
        "get_wikidata_item_details_by_id",
        get_wikidata_item_details_by_id,
    )
