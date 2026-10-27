import pytest
from ldap3.core.exceptions import LDAPSocketOpenError

from mex.backend.ldap.connector import LDAPConnector

# re-export the shared `mocked_ldap` fixture (and the canned LDAP dataset it patches
# `LDAPConnector` with) from `tests/auxiliary/conftest.py` - connector-level tests need
# the exact same dataset the `/ldap` route tests use, since both run against this
# repo's real LDAP mock server (`slapd`, seeded from `assets/raw-data/ldap/`) whenever
# that's reachable.
from tests.auxiliary.conftest import mocked_ldap  # noqa: F401


@pytest.fixture(autouse=True)
def skip_ldap_integration_tests_on_connection_error(
    is_integration_test: bool,  # noqa: FBT001
) -> None:
    if is_integration_test:
        try:
            LDAPConnector.get()
        except LDAPSocketOpenError:
            pytest.skip("LDAP unavailable")
