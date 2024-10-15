from collections import Counter
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


def count_results(persons: list) -> dict:
    # result=[]  # noqa: ERA001
    # result.append(person for person in persons if person["familyName"] == search_string)  # noqa: ERA001
    # return len(result)  # noqa: ERA001
    return Counter(persons)


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
    [("Mueller", 200, 2), ("Example", 200, 1), ("", 422, 0), ("None-Existent", 200, 0)],
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
    data = response.json()
    if response.status_code == 200:
        assert data["total"] == 3
    # TODO: count persons matched with search string
    assert count_results(list(data["items"]))[search_string] == match_total
