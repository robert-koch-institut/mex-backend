from mex.backend.cache import get_cache_connector
from mex.backend.identity.helpers import (
    get_identity_cache_key,
    reset_identity_cache,
)
from mex.common.identity import Identity
from mex.common.types import Identifier


def _identity(identifier_in_primary_source: str) -> Identity:
    return Identity(
        hadPrimarySource=Identifier("psSti00000000001"),
        identifier=Identifier.generate(seed=1),
        identifierInPrimarySource=identifier_in_primary_source,
        stableTargetId=Identifier.generate(seed=2),
    )


def test_get_identity_cache_key() -> None:
    assert get_identity_cache_key("psSti00000000001", "item-1") == (
        "psSti00000000001\nitem-1"
    )


def test_reset_identity_cache() -> None:
    cache = get_cache_connector()
    moved = _identity("moved-item")
    kept = _identity("untouched-item")
    moved_key = get_identity_cache_key(
        moved.hadPrimarySource, moved.identifierInPrimarySource
    )
    kept_key = get_identity_cache_key(
        kept.hadPrimarySource, kept.identifierInPrimarySource
    )
    cache.set_value(moved_key, moved)
    cache.set_value(kept_key, kept)

    reset_identity_cache([moved])

    assert cache.get_value(moved_key) is None
    assert cache.get_value(kept_key) is not None


def test_reset_identity_cache_without_moved_items() -> None:
    cache = get_cache_connector()
    kept = _identity("untouched-item")
    kept_key = get_identity_cache_key(
        kept.hadPrimarySource, kept.identifierInPrimarySource
    )
    cache.set_value(kept_key, kept)

    reset_identity_cache([])

    assert cache.get_value(kept_key) is not None
