from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from mex.common.models import AnyExtractedModel


@pytest.mark.integration
def test_get_merged_person_from_login(
    client_with_basic_auth_write_permission: TestClient,
    load_dummy_data: dict[str, AnyExtractedModel],
) -> None:
    person_1 = load_dummy_data["person_1"]

    with (
        patch("mex.backend.security.Connection") as mock_connection,
        patch("mex.backend.ldap.main.LDAPConnector.get") as mock_ldap_connector_get,
        patch("mex.backend.ldap.main.get_provider") as mock_get_provider,
    ):
        mocked_connection = mock_connection.return_value.__enter__.return_value
        mocked_connection.server.check_availability.return_value = True

        mock_ldap_connector = MagicMock()
        mock_ldap_connector.get_person.return_value = MagicMock(
            objectGUID=person_1.identifier
        )
        mock_ldap_connector_get.return_value = mock_ldap_connector

        mock_provider = MagicMock()
        mock_provider.fetch.side_effect = [
            [MagicMock(stableTargetId=person_1.hadPrimarySource)],
            [MagicMock(stableTargetId=person_1.stableTargetId)],
        ]
        mock_get_provider.return_value = mock_provider

        response = client_with_basic_auth_write_permission.post(
            "/v0/merged-person-from-login",
        )
        assert response.status_code == 200
        result = response.json()
        assert result is not None
        assert result["identifier"] == person_1.stableTargetId
