from fastapi.security import HTTPBasicCredentials

from mex.backend.testing.security import is_ldap_authenticated_mocked

write_credentials = HTTPBasicCredentials(
    username="Writer@rki.de",
    password="write_password",  # noqa: S106
)


def test_is_ldap_authenticated_mocked_success() -> None:
    # Should return the escaped username part
    result = is_ldap_authenticated_mocked(credentials=write_credentials)
    assert result == "Writer"
