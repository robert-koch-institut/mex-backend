import secrets
from enum import Enum

from mex.backend.types import (
    APIKey,
    APIKeyDatabase,
    APIUserDatabase,
    APIUserPassword,
    DynamicStrEnum,
)


def test_api_key_database() -> None:
    data_dict = {"read": "foo", "write": "bar"}
    db = APIKeyDatabase(**data_dict)

    assert db.read == [APIKey("foo")]


def test_api_key_repr() -> None:
    key = APIKey(secrets.token_urlsafe())
    key_repr = repr(key)
    assert key_repr == "APIKey('**********')"


def test_api_user_pw_repr() -> None:
    key = APIUserPassword(secrets.token_urlsafe())
    key_repr = repr(key)
    assert key_repr == "APIUserPassword('**********')"


def test_api_user_database() -> None:
    data_dict = {"read": {"foo": "test-pw"}}
    db = APIUserDatabase(**data_dict)
    assert db.read["foo"].get_secret_value() == "test-pw"
    assert db.read == {"foo": APIUserPassword("test-pw")}


def test_dynamic_str_enum() -> None:
    class Dummy(Enum, metaclass=DynamicStrEnum):
        __names__ = ["foo", "Bar", "LoremIpsum"]

    assert {d.name: str(d.value) for d in Dummy} == {
        "FOO": "foo",
        "BAR": "Bar",
        "LOREM_IPSUM": "LoremIpsum",
    }
