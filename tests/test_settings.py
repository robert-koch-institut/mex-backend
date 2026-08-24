import pytest
from pytest import MonkeyPatch

from mex.backend.settings import BackendSettings
from mex.common.types import IdentityProvider


def test_settings_require_graph_identity_provider(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("MEX_IDENTITY_PROVIDER", IdentityProvider.MEMORY.value)
    with pytest.raises(ValueError, match=r"Identity provider must be set to graph\."):
        BackendSettings()
