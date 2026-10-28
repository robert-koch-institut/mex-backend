import json
import os
from typing import TYPE_CHECKING
from unittest.mock import MagicMock
from uuid import UUID

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

import pytest
from pytest import MonkeyPatch

from mex.backend.ldap.connector import LDAPConnector
from mex.common.exceptions import MExError
from mex.common.ldap.models import LDAPActor, LDAPFunctionalAccount, LDAPPerson
from mex.common.models import PaginatedItemsContainer
from mex.common.settings import BaseSettings
from mex.common.transform import MExEncoder, normalize

test_persons_ldap = [
    LDAPPerson(
        employeeID="abc",
        sn="Mueller",
        givenName=["Max"],
        objectGUID=UUID(version=4, int=432),
        department="FG99",
        displayName="Max Mueller",
        mail=["MaxM@ldapmock.local"],
        sAMAccountName="MaxM",
    ),
    LDAPPerson(
        employeeID="def",
        sn="Example",
        givenName=["Moritz"],
        objectGUID=UUID(version=4, int=789),
        department="FG99",
        displayName="Moritz Example",
        mail=["MoritzE@ldapmock.local"],
        sAMAccountName="MoritzE",
    ),
    LDAPPerson(
        employeeID="ghi",
        sn="Mueller",
        givenName=["Moritz"],
        objectGUID=UUID(version=4, int=321),
        department="FG99",
        displayName="Moritz Mueller",
        mail=["MoritzM@ldapmock.local"],
        sAMAccountName="MoritzM",
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


def ldap_mock_searcher(
    actors: Sequence[LDAPActor],
) -> Callable[..., PaginatedItemsContainer[LDAPActor]]:
    """Create a mocked search with token matching and pagination support."""

    def tokenize(obj: object) -> set[str]:
        serialized = json.dumps(obj, cls=MExEncoder, ensure_ascii=False)
        return set(normalize(serialized.lower()).split())

    def mock_search(
        _self: LDAPConnector,
        *,
        query: str = "*",
        offset: int = 0,
        limit: int = 10,
    ) -> PaginatedItemsContainer[LDAPActor]:
        tokenized_query = tokenize(query)
        actors_by_score = [
            (len(tokenized_query & tokenize(actor)), actor) for actor in actors
        ]
        matched = [i[1] for i in sorted(actors_by_score, key=lambda i: i[0])]
        return PaginatedItemsContainer[LDAPActor](
            items=matched[offset : offset + limit],
            total=len(matched),
        )

    return mock_search


# mapping from `get_persons`/`get_functional_accounts` search kwargs to the LDAP
# attributes they filter on
_SEARCH_ATTR_BY_KWARG = {
    "display_name": "displayName",
    "employee_id": "employeeID",
    "given_name": "givenName",
    "mail": "mail",
    "object_guid": "objectGUID",
    "sam_account_name": "sAMAccountName",
    "surname": "sn",
}


def _attribute_values(actor: LDAPActor, attribute: str) -> list[str]:
    """Return an actor's attribute values as a list of strings."""
    value = getattr(actor, attribute, None)
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    return [str(value)]


def _actor_matches_filters(actor: LDAPActor, **kwargs: str) -> bool:
    """Check whether an actor matches all non-wildcard search filters."""
    for kwarg, value in kwargs.items():
        if value == "*":
            continue
        attribute = _SEARCH_ATTR_BY_KWARG[kwarg]
        values = [v.lower() for v in _attribute_values(actor, attribute)]
        if value.lower() not in values:
            return False
    return True


def ldap_mock_attribute_searcher(
    actors: Sequence[LDAPActor],
) -> Callable[..., PaginatedItemsContainer[LDAPActor]]:
    """Create a mocked search with exact attribute matching and pagination."""

    def mock_search(
        _self: LDAPConnector,
        *,
        offset: int = 0,
        limit: int = 10,
        **kwargs: str,
    ) -> PaginatedItemsContainer[LDAPActor]:
        matched = [actor for actor in actors if _actor_matches_filters(actor, **kwargs)]
        return PaginatedItemsContainer[LDAPActor](
            items=matched[offset : offset + limit],
            total=len(matched),
        )

    return mock_search


@pytest.fixture(params=["ldap_patched_connector", "ldap_mock_server"])
def mocked_ldap(request: pytest.FixtureRequest, monkeypatch: MonkeyPatch) -> None:
    if request.param == "ldap_patched_connector":
        monkeypatch.setattr(
            LDAPConnector,
            "__init__",
            lambda self: setattr(self, "_connection", MagicMock()),
        )
        monkeypatch.setattr(
            LDAPConnector,
            "get_persons",
            ldap_mock_attribute_searcher(test_persons_ldap),
        )
        monkeypatch.setattr(
            LDAPConnector,
            "get_functional_accounts",
            ldap_mock_attribute_searcher(test_accounts_ldap),
        )
        test_persons_and_functional_accounts_ldap = sorted(
            [*test_persons_ldap, *test_accounts_ldap], key=lambda x: x.objectGUID
        )
        monkeypatch.setattr(
            LDAPConnector,
            "get_persons_or_functional_accounts",
            ldap_mock_searcher(test_persons_and_functional_accounts_ldap),
        )
    elif request.param == "ldap_mock_server":
        if "MEX_LDAP_SEARCH_BASE" not in os.environ:
            pytest.skip("LDAP mock server not configured")
        else:
            # the mock server uses a self-signed certificate, so disable TLS
            # verification to allow connecting to it during testing
            monkeypatch.setattr(BaseSettings.get(), "verify_session", False)


@pytest.fixture(autouse=True)
def skip_ldap_integration_tests_on_connection_error(
    is_integration_test: bool,  # noqa: FBT001
) -> None:
    if is_integration_test:
        try:
            LDAPConnector.get()
        except MExError:
            pytest.skip("LDAP unavailable")
