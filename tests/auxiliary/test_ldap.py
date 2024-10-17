from uuid import UUID

import pytest
from fastapi.testclient import TestClient
from pytest import MonkeyPatch

from mex.common.ldap.models.person import LDAPPerson
from mex.common.ldap.transform import (
    transform_ldap_persons_to_mex_persons,
)
from mex.common.models import (
    ExtractedOrganizationalUnit,
    ExtractedPrimarySource,
)
from mex.common.testing import Joker


def count_results(search_string: str, persons: list) -> tuple:
    return sum(
        1
        for person in persons
        if search_string in person.get("givenName", [])
        or search_string in person.get("familyName", [])
    )


def test_transform_ldap_persons_to_mex_persons(
    extracted_unit: ExtractedOrganizationalUnit,
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
) -> None:
    ldap_person = LDAPPerson(
        company="RKI",
        department="MF",
        departmentNumber="MF4",
        displayName="Sample, Sam, Dr.",
        employeeID="SampleS",
        givenName="Sam",
        mail=["mail@example2.com"],
        objectGUID=UUID(int=42, version=4),
        ou="MF",
        sAMAccountName="samples",
        sn="Sample",
    )
    extracted_persons = transform_ldap_persons_to_mex_persons(
        [ldap_person], extracted_primary_sources["ldap"], [extracted_unit]
    )
    extracted_person = next(iter(extracted_persons))
    expected = {
        "email": ["mail@example2.com"],
        "familyName": ["Sample"],
        "fullName": ["Sample, Sam, Dr."],
        "givenName": ["Sam"],
        "hadPrimarySource": extracted_primary_sources["ldap"].stableTargetId,
        "identifier": Joker(),
        "identifierInPrimarySource": "00000000-0000-4000-8000-00000000002a",
        "memberOf": [extracted_unit.stableTargetId],
        "stableTargetId": Joker(),
    }
    assert (
        extracted_person.model_dump(exclude_none=True, exclude_defaults=True)
        == expected
    )


@pytest.mark.parametrize(
    "search_string, status_code, match_total",
    [
        ("Mueller", 200, 2),
        ("Example", 200, 1),
        ("", 422, None),
        ("None-Existent", 200, 0),
    ],
    ids=[
        "Get existing Person with same name",
        "Get existing Person with unique name",
        "Empty Search String",
        "Non-existent string",
    ],
)
@pytest.mark.usefixtures("mocked_ldap")
def test_search_persons_in_ldap_mocked(
    client_with_api_key_read_permission: TestClient,
    monkeypatch: MonkeyPatch,
    search_string,
    status_code,
    match_total,
) -> None:
    response = client_with_api_key_read_permission.get(
        "/v0/ldap", params={"q": search_string}
    )
    assert response.status_code == status_code
    if response.status_code == 200:
        data = response.json()
        assert data["total"] == 3
        assert count_results(search_string, data["items"]) == match_total
