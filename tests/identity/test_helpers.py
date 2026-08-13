import pytest

from mex.backend.cache.connector import CacheConnector
from mex.backend.identity.helpers import (
    get_identity_cache_key,
    reset_identity_cache,
)
from mex.common.identity import Identity
from mex.common.types import Identifier


def test_get_identity_cache_key() -> None:
    assert get_identity_cache_key("psSti00000000001", "item-1") == (
        "psSti00000000001\nitem-1"
    )


@pytest.mark.usefixtures("mocked_valkey")
def test_reset_identity_cache() -> None:
    cache = CacheConnector.get()
    moved_key = get_identity_cache_key("psSti00000000001", "moved-item")
    kept_key = get_identity_cache_key("psSti00000000001", "untouched-item")
    for key in (moved_key, kept_key):
        cache.set_value(
            key,
            Identity(
                hadPrimarySource=Identifier("psSti00000000001"),
                identifier=Identifier.generate(seed=1),
                identifierInPrimarySource=key.split("\n")[1],
                stableTargetId=Identifier.generate(seed=2),
            ),
        )

    reset_identity_cache(
        [
            {
                "hadPrimarySource": "psSti00000000001",
                "identifierInPrimarySource": "moved-item",
            }
        ]
    )

    assert cache.get_value(moved_key) is None
    assert cache.get_value(kept_key) is not None


@pytest.mark.usefixtures("mocked_valkey")
def test_reset_identity_cache_without_moved_items() -> None:
    cache = CacheConnector.get()
    key = get_identity_cache_key("psSti00000000001", "untouched-item")
    cache.set_value(
        key,
        Identity(
            hadPrimarySource=Identifier("psSti00000000001"),
            identifier=Identifier.generate(seed=1),
            identifierInPrimarySource="untouched-item",
            stableTargetId=Identifier.generate(seed=2),
        ),
    )

    reset_identity_cache([])

    assert cache.get_value(key) is not None
