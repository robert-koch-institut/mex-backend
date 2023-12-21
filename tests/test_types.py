import secrets

from mex.backend.types import APIKey, APIKeyDatabase


def test_user_database() -> None:
    data_dict = {"read": "foo", "write": "bar"}
    db = APIKeyDatabase(**data_dict)

    assert db.read == [APIKey("foo")]


def test_api_key_repr() -> None:
    key = APIKey(secrets.token_urlsafe())
    key_repr = repr(key)
    assert key_repr == "APIKey('**********')"
