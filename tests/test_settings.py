import pytest
from pytest import MonkeyPatch

from mex.backend.settings import BackendSettings
from mex.backend.types import CacheConnectorType
from mex.common.types import IdentityProvider


def test_settings_require_graph_identity_provider(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("MEX_IDENTITY_PROVIDER", IdentityProvider.MEMORY.value)
    with pytest.raises(ValueError, match=r"Identity provider must be set to graph\."):
        BackendSettings()


def test_settings_require_shared_cache_when_parallelized(
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setenv("MEX_BACKEND_API_PARALLELIZATION", "2")
    monkeypatch.setenv("MEX_BACKEND_CACHE_CONNECTOR", CacheConnectorType.MEMORY.value)
    with pytest.raises(ValueError, match=r"cache connector must be set to valkey\."):
        BackendSettings()
