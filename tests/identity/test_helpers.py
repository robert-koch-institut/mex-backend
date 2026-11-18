from mex.backend.identity.helpers import get_identity_cache_key


def test_get_identity_cache_key() -> None:
    assert get_identity_cache_key("psSti00000000001", "item-1") == (
        "psSti00000000001\nitem-1"
    )
