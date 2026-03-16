from enum import Enum, EnumMeta, _EnumDict

from pydantic import ConfigDict, SecretStr, field_validator

from mex.common.fields import ALL_REFERENCE_FIELD_NAMES
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


class APIKeyDatabase(BaseModel):
    """A lookup from access level to list of allowed APIKeys."""

    read: list[APIKey] = []
    write: list[APIKey] = []


class OIDCGroupsDatabase(BaseModel):
    """A lookup from access level to list of allowed OIDC group names."""

    read: list[str] = []
    write: list[str] = []


class OIDCClaims(BaseModel):
    """Parsed and validated claims from an OIDC JWT."""

    model_config = ConfigDict(extra="ignore")

    preferred_username: str = ""
    groups: list[str] = []

    @field_validator("groups", mode="before")
    @classmethod
    def coerce_null_groups(cls, v: object) -> object:
        """Treat a null groups claim the same as an absent one."""
        return [] if v is None else v


class DynamicStrEnum(EnumMeta):
    """Metaclass to dynamically populate an enumeration from a list of strings."""

    def __new__(
        cls: type[DynamicStrEnum], name: str, bases: tuple[type], dct: _EnumDict
    ) -> DynamicStrEnum:
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
