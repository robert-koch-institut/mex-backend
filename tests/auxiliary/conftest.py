import json
from pathlib import Path
from unittest.mock import MagicMock, Mock
from uuid import UUID

import pytest
import requests
from pytest import MonkeyPatch
from requests import Response

from mex.common.wikidata.connector import (
    WikidataAPIConnector,
    WikidataQueryServiceConnector,
)

from mex.common.ldap.connector import(
    LDAPConnector
)
from mex.common.ldap.extract import(
    get_count_of_found_persons_by_name, get_persons_by_name
)
from mex.common.models import ExtractedPerson, ExtractedPrimarySource

TEST_DATA_DIR = Path(__file__).parent / "test_data"


@pytest.fixture()
def mocked_ldap(monkeypatch: MonkeyPatch) -> None:
    response_query = Mock(spec=Response, status_code=200)

    session = MagicMock(spec=requests.Session)
    session.get = MagicMock(side_effect=[response_query])

    def mocked_init(self: LDAPConnector) -> None:
        self.session = session
    # Mock für die Funktion get_count_of_found_persons_by_name
    monkeypatch.setattr(get_count_of_found_persons_by_name, "get_count_of_found_persons_by_name", MagicMock(return_value=1))

    # Mock für die Funktion get_persons_by_name
    mock_persons = [
        {
        "company": "RKI",
        "department": "XY",
        "departmentNumber": "XY2",
        "displayName": "Sample, Sam",
        "employeeID": "1024",
        "givenName": ["Sam"],
        "mail": ["SampleS@mail.tld"],
        "objectGUID": UUID(int=0, version=4),
        "ou": ["XY"],
        "sAMAccountName": "SampleS",
        "sn": "Sample",
    }
    ]
    monkeypatch.setattr(get_persons_by_name, "get_persons_by_name", MagicMock(return_value=mock_persons))

    # Mock für die Funktion transform_ldap_persons_to_mex_persons
    # def mock_transform_ldap_persons_to_mex_persons(persons, primary_source, organizational_units):
    #     return [
    #         ExtractedPerson(name=person["name"], id=person["id"]) for person in persons[0]
    #     ]

    # monkeypatch.setattr(LDAPConnector, "transform_ldap_persons_to_mex_persons", mock_transform_ldap_persons_to_mex_persons)

    # Mock für die Funktion extracted_primary_source_ldap
    # def mock_extracted_primary_source_ldap() -> ExtractedPrimarySource:
    #     return ExtractedPrimarySource(
    #         hadPrimarySource="ldap_source_id",
    #         identifierInPrimarySource="ldap",
    #         title=["LDAP"],
    #         entityType="ExtractedPrimarySource",
    #     )

    # monkeypatch.setattr(LDAPConnector, "extracted_primary_source_ldap", mock_extracted_primary_source_ldap)

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