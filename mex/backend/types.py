from enum import Enum, EnumMeta, _EnumDict

from pydantic import SecretStr

from mex.backend.fields import (
    ALL_REFERENCE_FIELD_NAMES,
)
from mex.common.models import (
    EXTRACTED_MODEL_CLASSES_BY_NAME,
    MERGED_MODEL_CLASSES_BY_NAME,
    BaseModel,
)
from mex.common.transform import dromedary_to_snake


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


class Validation(Enum):
    """Defines possible validation strategies."""

    STRICT = "strict"
    LENIENT = "lenient"
    IGNORE = "ignore"


class DynamicStrEnum(EnumMeta):
    """Metaclass to dynamically populate an enumeration from a list of strings."""

    def __new__(
        cls: type["DynamicStrEnum"], name: str, bases: tuple[type], dct: _EnumDict
    ) -> "DynamicStrEnum":
        """Create a new enum by adding an entry for each name in the source."""
        for value in dct.pop("__names__"):
            dct[dromedary_to_snake(value).upper()] = value
        return super().__new__(cls, name, bases, dct)


class ExtractedType(Enum, metaclass=DynamicStrEnum):
    """Enumeration of possible types for extracted items."""

    __names__ = list(EXTRACTED_MODEL_CLASSES_BY_NAME)


class MergedType(Enum, metaclass=DynamicStrEnum):
    """Enumeration of possible types for merged items."""

    __names__ = list(MERGED_MODEL_CLASSES_BY_NAME)


class ReferenceFieldName(Enum, metaclass=DynamicStrEnum):
    """Enumeration of possible field names that contain references."""

    __names__ = sorted(ALL_REFERENCE_FIELD_NAMES)
