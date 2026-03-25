import json
import os
from pathlib import Path
from typing import Any, cast
from unittest.mock import MagicMock, Mock
from uuid import UUID

import pytest
import requests
from pytest import MonkeyPatch
from requests import Response

from mex.common.ldap.connector import LDAPConnector
from mex.common.ldap.models import AnyLDAPActor, LDAPFunctionalAccount, LDAPPerson
from mex.common.models import PaginatedItemsContainer
from mex.common.orcid.connector import OrcidConnector
from mex.common.orcid.models import OrcidRecord, OrcidSearchResponse
from mex.common.wikidata.connector import WikidataAPIConnector

TEST_DATA_DIR = Path(__file__).parent / "test_data"

test_persons_ldap = [
    LDAPPerson(
        employeeID="abc",
        sn="Mueller",
        givenName=["Max"],
        objectGUID=UUID(version=4, int=432),
        department="FG99",
        displayName="Max Mueller",
    ),
    LDAPPerson(
        employeeID="def",
        sn="Example",
        givenName=["Moritz"],
        objectGUID=UUID(version=4, int=789),
        department="FG99",
        displayName="Moritz Example",
    ),
    LDAPPerson(
        employeeID="ghi",
        sn="Mueller",
        givenName=["Moritz"],
        objectGUID=UUID(version=4, int=321),
        department="FG99",
        displayName="Moritz Mueller",
    ),
]

test_accounts_ldap = [
    LDAPFunctionalAccount(
        objectGUID=UUID(version=4, int=123),
        ou="Funktion",
        mail="help@account.test",
    ),
    LDAPFunctionalAccount(
        objectGUID=UUID(version=4, int=543),
        ou="Funktion",
        mail="info@mail.provider",
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


@pytest.fixture(params=["ldap_patched_connector", "ldap_mock_server"])
def mocked_ldap(request: pytest.FixtureRequest, monkeypatch: MonkeyPatch) -> None:
    if request.param == "ldap_patched_connector":

        def __init__(self: LDAPConnector) -> None:
            self._connection = MagicMock(extend=Mock())
            self._connection.extend.standard.paged_search = MagicMock(side_effect=[])

        monkeypatch.setattr(LDAPConnector, "__init__", __init__)

        monkeypatch.setattr(
            LDAPConnector,
            "get_persons",
            MagicMock(
                return_value=PaginatedItemsContainer[LDAPPerson](
                    items=test_persons_ldap, total=len(test_persons_ldap)
                )
            ),
        )

        monkeypatch.setattr(
            LDAPConnector,
            "get_functional_accounts",
            MagicMock(
                return_value=PaginatedItemsContainer[LDAPFunctionalAccount](
                    items=test_accounts_ldap, total=len(test_accounts_ldap)
                )
            ),
        )

        test_persons_and_functional_accounts_ldap = sorted(
            [*test_persons_ldap, *test_accounts_ldap], key=lambda x: x.objectGUID
        )
        monkeypatch.setattr(
            LDAPConnector,
            "get_persons_or_functional_accounts",
            MagicMock(
                return_value=PaginatedItemsContainer[AnyLDAPActor](
                    items=test_persons_and_functional_accounts_ldap,
                    total=len(test_persons_and_functional_accounts_ldap),
                ),
            ),
        )
    elif request.param == "ldap_mock_server":
        if "MEX_LDAP_SEARCH_BASE" not in os.environ:
            pytest.skip("LDAP mock server not configured")
        else:
            # TODO(ND): Make this configurable in mex-common

            from mex.common.ldap import connector as connector_module  # noqa: PLC0415

            monkeypatch.setattr(connector_module, "AUTO_BIND_NO_TLS", "DEFAULT")


@pytest.fixture
def mocked_wikidata(monkeypatch: MonkeyPatch) -> None:
    response_query = Mock(spec=Response, status_code=200)

    session = MagicMock(spec=requests.Session)
    session.get = MagicMock(side_effect=[response_query])

    def mocked_init(self: WikidataAPIConnector) -> None:
        self.session = session

    monkeypatch.setattr(WikidataAPIConnector, "__init__", mocked_init)

    # mock get_wikidata_org_with_org_id
    with (TEST_DATA_DIR / "wikidata_organization_raw.json").open() as fh:
        wikidata_organization_raw = json.load(fh)

    def get_wikidata_item_details_by_id(
        _self: WikidataAPIConnector,
        _item_id: str,
    ) -> dict[str, str]:
        return cast("dict[str, str]", wikidata_organization_raw)

    monkeypatch.setattr(
        WikidataAPIConnector,
        "get_wikidata_item_details_by_id",
        get_wikidata_item_details_by_id,
    )


@pytest.fixture
def orcid_person_raw() -> dict[str, Any]:
    """Return a raw orcid person."""
    with (TEST_DATA_DIR / "orcid_person_raw.json").open() as fh:
        return cast("dict[str, Any]", json.load(fh))


@pytest.fixture
def orcid_multiple_matches() -> dict[str, Any]:
    """Return a raw orcid person."""
    with (TEST_DATA_DIR / "orcid_multiple_matches.json").open() as fh:
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
        _self: OrcidConnector,
        given_and_family_names: str | None = None,
        **_: Any,  # noqa: ANN401
    ) -> OrcidSearchResponse:
        if given_and_family_names in {"John Doe", "John O'Doe"}:
            return OrcidSearchResponse(num_found=1, result=[orcid_person_raw])
        if given_and_family_names == "Multiple Doe":
            return OrcidSearchResponse.model_validate(orcid_multiple_matches)
        return OrcidSearchResponse(num_found=0, result=[])

    monkeypatch.setattr(
        OrcidConnector, "search_records_by_name", search_records_by_name
    )

    def get_record_by_id(_self: OrcidConnector, _orcid_id: str) -> OrcidRecord:
        return OrcidRecord.model_validate(orcid_person_raw)

    monkeypatch.setattr(OrcidConnector, "get_record_by_id", get_record_by_id)
