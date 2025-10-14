from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from mex.common.models import MergedPerson
from mex.common.types import Email, MergedPersonIdentifier


def test_get_merged_person_from_login(
    client_with_basic_auth_write_permission: TestClient,
) -> None:
    with (
        patch("mex.backend.security.Connection") as mock_connection,
        patch("mex.backend.ldap.main.LDAPConnector.get") as mock_ldap_connector_get,
        patch("mex.backend.ldap.main.get_provider") as mock_get_provider,
        patch("mex.backend.ldap.main.get_merged_item") as mock_get_merged_item,
    ):
        mocked_connection = mock_connection.return_value.__enter__.return_value
        mocked_connection.server.check_availability.return_value = True

        mock_ldap_connector = MagicMock()
        mock_ldap_connector.get_person.return_value = MagicMock(
            objectGUID="bFQoRhcVH5DHUI"
        )
        mock_ldap_connector_get.return_value = mock_ldap_connector
        mock_extracted_primary_source_ldap = MagicMock()
        mock_extracted_primary_source_ldap.stableTargetId = "mocked-primary-source-id"
        mock_provider = MagicMock()
        mock_provider.fetch.side_effect = [
            [MagicMock(stableTargetId="bFQoRhcVH5DHUI")],
        ]
        mock_get_provider.return_value = mock_provider

        mock_person = MergedPerson(
            email=[Email("person_1@example.com")],
            fullName=["Bernd, Brot"],
            identifier=MergedPersonIdentifier("bFQoRhcVH5DHUI"),
            entityType="MergedPerson",
        )
        mock_get_merged_item.return_value = mock_person

        response = client_with_basic_auth_write_permission.post(
            "/v0/merged-person-from-login",
        )
        assert response.status_code == 200
        result = response.json()
        assert result is not None
        assert result["identifier"] == "bFQoRhcVH5DHUI"
        assert result["email"] == ["person_1@example.com"]
        assert result["fullName"] == ["Bernd, Brot"]
