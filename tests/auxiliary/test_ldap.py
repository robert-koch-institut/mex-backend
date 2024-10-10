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
    "search_string, status_code, expected_total",
    [("Mueller", 200, 2), ("Example", 200, 1), (str(UUID(version=4, int=3)), 200, 1)],
)
@pytest.mark.usefixtures("mocked_ldap")
def test_search_persons_in_ldap_mocked(
    client_with_api_key_read_permission: TestClient,
    monkeypatch: MonkeyPatch,
    search_string,
    status_code,
    expected_total,
) -> None:
    response = client_with_api_key_read_permission.get(
        "/v0/ldap", params={"q": search_string}
    )
    assert response.status_code == status_code
    data = response.json()
    # assert sum(1 for person in data if (person["sn"] == search_string | person["objectGUID"] == search_string)) == expected_total  # noqa: ERA001
    assert (
        sum(
            1
            for person in data
            if person["sn"] == search_string or person["objectGUID"] == search_string
        )
        == expected_total
    )
    assert data["total"] == expected_total
    assert len(data["items"]) == expected_total
