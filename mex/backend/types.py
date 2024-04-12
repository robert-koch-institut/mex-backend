from enum import Enum, EnumMeta, _EnumDict
from typing import Literal

from pydantic import SecretStr

from mex.common.models import (
    EXTRACTED_MODEL_CLASSES_BY_NAME,
    MERGED_MODEL_CLASSES_BY_NAME,
    BaseModel,
)
from mex.common.transform import dromedary_to_snake

LiteralStringType = type(Literal["str"])


class AccessLevel(Enum):
    """Enum of access level."""

    READ = "read"
    WRITE = "write"


class APIKey(SecretStr):
    """An API Key used for authenticating and authorizing a client."""

    def __repr__(self) -> str:
        """Return a secure representation of this key."""
        return f"APIKey('{self}')"


class APIUserPassword(SecretStr):
    """An API password used for basic authentication along with a username."""


class APIKeyDatabase(BaseModel):
    """A lookup from access level to list of allowed APIKeys."""

    read: list[APIKey] = []
    write: list[APIKey] = []


class APIUserDatabase(BaseModel):
    """Database containing usernames and passwords for backend API."""

    read: dict[str, APIUserPassword] = {}
    write: dict[str, APIUserPassword] = {}


class BackendIdentityProvider(Enum):
    """Identity providers implemented by mex-backend."""

    GRAPH = "graph"


class DynamicStrEnum(EnumMeta):
    """Meta class to dynamically populate the an enumeration from a list of strings."""

    def __new__(
        cls: type["DynamicStrEnum"], name: str, bases: tuple[type], dct: _EnumDict
    ) -> "DynamicStrEnum":
        """Create a new enum by adding an entry for each name in the source."""
        for name in dct.pop("__names__"):
            dct[dromedary_to_snake(name).upper()] = name
        return super().__new__(cls, name, bases, dct)


class UnprefixedType(Enum, metaclass=DynamicStrEnum):
    """Enumeration of possible types without any prefix."""

    __names__ = [m.removeprefix("Extracted") for m in EXTRACTED_MODEL_CLASSES_BY_NAME]


class ExtractedType(Enum, metaclass=DynamicStrEnum):
    """Enumeration of possible types for extracted items."""

    __names__ = list(EXTRACTED_MODEL_CLASSES_BY_NAME)


class MergedType(Enum, metaclass=DynamicStrEnum):
    """Enumeration of possible types for merged items."""

    __names__ = list(MERGED_MODEL_CLASSES_BY_NAME)
