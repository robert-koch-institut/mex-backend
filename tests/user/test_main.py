from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

from mex.common.models import MergedPerson
from mex.common.types import MergedPersonIdentifier

if TYPE_CHECKING:  # pragma: no cover
    from fastapi.testclient import TestClient


def test_get_current_user(
    client_with_bearer_write_permission: TestClient,
) -> None:
    with (
        patch("mex.backend.security._get_jwks_client") as mock_get_jwks_client,
        patch(
            "mex.backend.security.jwt.decode",
            return_value={"preferred_username": "Writer", "groups": ["Abteilung_21"]},
        ),
        patch("mex.backend.user.main.LDAPConnector.get") as mock_ldap_connector_get,
        patch("mex.backend.user.main.get_provider") as mock_get_provider,
        patch("mex.backend.user.main.get_merged_item") as mock_get_merged_item,
    ):
        mock_jwks_client = MagicMock()
        mock_jwks_client.get_signing_key_from_jwt.return_value = MagicMock(
            key="fake_key"
        )
        mock_get_jwks_client.return_value = mock_jwks_client

        mock_ldap_connector = MagicMock()
        mock_ldap_connector.get_person.return_value = MagicMock(
            objectGUID="bFQoRhcVH5DHUI"
        )
        mock_ldap_connector_get.return_value = mock_ldap_connector

        mock_provider = MagicMock()
        mock_provider.fetch.return_value = [MagicMock(stableTargetId="bFQoRhcVH5DHUI")]
        mock_get_provider.return_value = mock_provider

        mock_person = MergedPerson(
            email=["person_1@example.com"],
            fullName=["Bernd, Brot"],
            identifier=MergedPersonIdentifier("bFQoRhcVH5DHUI"),
            entityType="MergedPerson",
        )
        mock_get_merged_item.return_value = mock_person

        response = client_with_bearer_write_permission.get("/v0/user/me")
        assert response.status_code == 200
        result = response.json()
        assert result is not None
        assert result["identifier"] == "bFQoRhcVH5DHUI"
        assert result["email"] == ["person_1@example.com"]
        assert result["fullName"] == ["Bernd, Brot"]
