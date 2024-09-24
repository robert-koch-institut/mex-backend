import pytest
from uuid import UUID
from fastapi.testclient import TestClient
from pytest import MonkeyPatch

from mex.backend.auxiliary.ldap import extracted_primary_source_ldap
from mex.common.models import (
    ExtractedPrimarySource,
)
from mex.backend.auxiliary import ldap

from mex.common.models import ExtractedOrganizationalUnit, ExtractedPrimarySource
from mex.common.ldap.models.person import LDAPPerson
from mex.common.ldap.transform import (
    transform_ldap_persons_to_mex_persons,
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


@pytest.mark.usefixtures("mocked_ldap")
def test_search_persons_in_ldap_mocked(
    client_with_api_key_read_permission: TestClient, monkeypatch: MonkeyPatch
) -> None:
    def extracted_primary_source_ldap() -> ExtractedPrimarySource:
        return extracted_primary_source_ldap()

    monkeypatch.setattr(
        ldap, "extracted_primary_source_ldap", extracted_primary_source_ldap
    )
    response = client_with_api_key_read_permission.get(
        "/v0/ldap", params={"q": "Sample"}
    ).json()        
    assert response.status_code == 200
    data = response.json()
    assert "total" in data
    assert "items" in data
    #assert data["total"] == 1
    assert len(data["items"]) == 1
    assert data["items"][0]["givenName"] == "Sam"
