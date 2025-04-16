import json
from pathlib import Path
from typing import Any, cast
from unittest.mock import MagicMock, Mock
from uuid import UUID

import pytest
import requests
from ldap3 import Connection
from pytest import MonkeyPatch
from requests import Response

from mex.backend.auxiliary.organigram import extracted_organizational_unit
from mex.backend.auxiliary.primary_source import (
    extracted_primary_source_ldap,
    extracted_primary_source_orcid,
    extracted_primary_source_wikidata,
)
from mex.common.ldap.connector import LDAPConnector
from mex.common.ldap.models import LDAPPerson
from mex.common.orcid.connector import OrcidConnector
from mex.common.orcid.models import OrcidRecord, OrcidSearchResponse
from mex.common.wikidata.connector import WikidataAPIConnector

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

test_person_orcid = [
    {
        "orcid-identifier": {
            "uri": "https://orcid.org/0000-0002-1234-5678",
            "path": "0000-0002-1234-5678",
            "host": "orcid.org",
        },
        "person": {
            "name": {
                "given-names": {"value": "Jane"},
                "family-name": {"value": "Doe"},
                "visibility": "public",
                "path": "0000-0002-1234-5678",
            },
            "emails": {
                "email": [
                    "jane.doe@example.com",
                ],
            },
            "path": "/0000-0002-1234-5678/person",
        },
    }
]


@pytest.fixture
def mocked_ldap(monkeypatch: MonkeyPatch) -> None:
    def __init__(self: LDAPConnector) -> None:
        self._connection = MagicMock(spec=Connection, extend=Mock())
        self._connection.extend.standard.paged_search = MagicMock(side_effect=[])

    monkeypatch.setattr(LDAPConnector, "__init__", __init__)

    monkeypatch.setattr(
        LDAPConnector, "get_persons", MagicMock(return_value=test_persons)
    )


@pytest.fixture
def mocked_wikidata(monkeypatch: MonkeyPatch) -> None:
    response_query = Mock(spec=Response, status_code=200)

    session = MagicMock(spec=requests.Session)
    session.get = MagicMock(side_effect=[response_query])

    def mocked_init(self: WikidataAPIConnector) -> None:
        self.session = session

    monkeypatch.setattr(WikidataAPIConnector, "__init__", mocked_init)

    # mock get_wikidata_org_with_org_id
    with open(TEST_DATA_DIR / "wikidata_organization_raw.json") as fh:
        wikidata_organization_raw = json.load(fh)

    def get_wikidata_item_details_by_id(
        _self: WikidataAPIConnector,
        _item_id: str,
    ) -> dict[str, str]:
        return wikidata_organization_raw

    monkeypatch.setattr(
        WikidataAPIConnector,
        "get_wikidata_item_details_by_id",
        get_wikidata_item_details_by_id,
    )


@pytest.fixture
def orcid_person_raw() -> dict[str, Any]:
    """Return a raw orcid person."""
    with open(TEST_DATA_DIR / "orcid_person_raw.json") as fh:
        return cast("dict[str, Any]", json.load(fh))


@pytest.fixture
def orcid_multiple_matches() -> dict[str, Any]:
    """Return a raw orcid person."""
    with open(TEST_DATA_DIR / "orcid_multiple_matches.json") as fh:
        return cast("dict[str, Any]", json.load(fh))


@pytest.fixture
def mocked_orcid(
    monkeypatch: pytest.MonkeyPatch,
    orcid_person_raw: dict[str, Any],
    orcid_multiple_matches: dict[str, Any],
) -> None:
    response_query = Mock(spec=Response, status_code=200)
    session = MagicMock(spec=requests.Session)
    session.get = MagicMock(side_effect=[response_query])

    def __init__(self: OrcidConnector) -> None:
        self.session = session

    monkeypatch.setattr(OrcidConnector, "__init__", __init__)

    def search_records_by_name(
        _self: OrcidConnector, given_and_family_names: str | None = None, **_: Any
    ) -> OrcidSearchResponse:
        if given_and_family_names in {"John Doe", "John O'Doe"}:
            return OrcidSearchResponse(num_found=1, result=[orcid_person_raw])
        if given_and_family_names in {"Multiple Doe"}:
            return OrcidSearchResponse.model_validate(orcid_multiple_matches)
        return OrcidSearchResponse(num_found=0, result=[])

    monkeypatch.setattr(
        OrcidConnector, "search_records_by_name", search_records_by_name
    )

    def get_record_by_id(_self: OrcidConnector, _orcid_id: str) -> OrcidRecord:
        return OrcidRecord.model_validate(orcid_person_raw)

    monkeypatch.setattr(OrcidConnector, "get_record_by_id", get_record_by_id)


@pytest.fixture(autouse=True)
def seed_primary_sources(is_integration_test: bool) -> None:
    if is_integration_test:
        extracted_primary_source_ldap()
        extracted_primary_source_orcid()
        extracted_primary_source_wikidata()


@pytest.fixture(autouse=True)
def seed_organizational_units(is_integration_test: bool) -> None:
    if is_integration_test:
        extracted_organizational_unit()
