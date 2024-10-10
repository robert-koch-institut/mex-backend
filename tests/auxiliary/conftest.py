import json
from pathlib import Path
from unittest.mock import MagicMock, Mock
from uuid import UUID

import pytest
import requests
from ldap3 import Connection
from pytest import MonkeyPatch
from requests import Response

from mex.common.ldap.connector import LDAPConnector
from mex.common.ldap.models.person import LDAPPerson
from mex.common.models import (
    ExtractedOrganizationalUnit,
    ExtractedPrimarySource,
)
from mex.common.wikidata.connector import (
    WikidataAPIConnector,
    WikidataQueryServiceConnector,
)

TEST_DATA_DIR = Path(__file__).parent / "test_data"

test_persons = [
    LDAPPerson(
        employeeID="abc",
        sn="Mueller",
        givenName=["Max"],
        objectGUID=UUID(version=4, int=1),
        department="FG99",
    ),
    LDAPPerson(
        employeeID="def",
        sn="Example",
        givenName=["Moritz"],
        objectGUID=UUID(version=4, int=2),
        department="FG99",
    ),
    LDAPPerson(
        employeeID="ghi",
        sn="Mueller",
        givenName=["Moritz"],
        objectGUID=UUID(version=4, int=3),
        department="FG99",
    ),
]


@pytest.fixture()
def mocked_ldap(monkeypatch: MonkeyPatch) -> None:
    def __init__(self: LDAPConnector) -> None:
        self._connection = MagicMock(spec=Connection, extend=Mock())
        self._connection.extend.standard.paged_search = MagicMock(side_effect=[])

    monkeypatch.setattr(LDAPConnector, "__init__", __init__)
    monkeypatch.setattr(
        LDAPConnector, "get_persons", MagicMock(return_value=test_persons)
    )


@pytest.fixture()
def extracted_unit(
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
) -> ExtractedOrganizationalUnit:
    return ExtractedOrganizationalUnit(
        name=["MF"],
        hadPrimarySource=extracted_primary_sources["ldap"].stableTargetId,
        identifierInPrimarySource="mf",
    )


@pytest.fixture()
def mocked_wikidata(monkeypatch: MonkeyPatch) -> None:
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
    with open(TEST_DATA_DIR / "wikidata_organization_raw.json") as fh:
        wikidata_organization_raw = json.load(fh)

    def get_wikidata_item_details_by_id(
        self: WikidataQueryServiceConnector, item_id: str
    ) -> dict[str, str]:
        return wikidata_organization_raw

    monkeypatch.setattr(
        WikidataAPIConnector,
        "get_wikidata_item_details_by_id",
        get_wikidata_item_details_by_id,
    )
