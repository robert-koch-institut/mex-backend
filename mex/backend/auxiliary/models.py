from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Discriminator, Field, Tag

LDAPAttributeValue = Annotated[str, Field(max_length=1000)]


class LDAPPersonLookup(BaseModel):
    """Request body for looking up LDAP persons by exact attribute values.

    Every attribute defaults to `"*"` and may contain `*` wildcards. `entityType` is a
    list, so that it can later be widened to more entity types without breaking
    existing requests.
    """

    model_config = ConfigDict(extra="forbid")

    entityType: Annotated[
        list[Literal["ExtractedPerson"]], Field(min_length=1, max_length=1)
    ]
    displayName: LDAPAttributeValue = "*"
    employeeId: LDAPAttributeValue = "*"
    givenName: LDAPAttributeValue = "*"
    mail: LDAPAttributeValue = "*"
    objectGuid: LDAPAttributeValue = "*"
    samAccountName: LDAPAttributeValue = "*"
    surname: LDAPAttributeValue = "*"
    skip: Annotated[int, Field(ge=0, le=10e10)] = 0
    limit: Annotated[int, Field(ge=1, le=100)] = 10


class LDAPFunctionalAccountLookup(BaseModel):
    """Request body for looking up LDAP functional accounts by mail address.

    `mail` defaults to `"*"` and may contain `*` wildcards. `entityType` is a list, so
    that it can later be widened to more entity types without breaking existing
    requests.
    """

    model_config = ConfigDict(extra="forbid")

    entityType: Annotated[
        list[Literal["ExtractedContactPoint"]], Field(min_length=1, max_length=1)
    ]
    mail: LDAPAttributeValue = "*"
    skip: Annotated[int, Field(ge=0, le=10e10)] = 0
    limit: Annotated[int, Field(ge=1, le=100)] = 10


def _lookup_tag(value: object) -> str | None:
    """Pick the lookup shape from a body's single-item `entityType` list.

    Returns `None` for anything else, which pydantic rejects as a validation error.
    """
    if not isinstance(value, dict):
        return None
    entity_type = value.get("entityType")
    if isinstance(entity_type, list) and len(entity_type) == 1:
        return str(entity_type[0])
    return None


AnyLDAPLookup = Annotated[
    Annotated[LDAPPersonLookup, Tag("ExtractedPerson")]
    | Annotated[LDAPFunctionalAccountLookup, Tag("ExtractedContactPoint")],
    Discriminator(
        _lookup_tag,
        custom_error_type="invalid_entity_type",
        custom_error_message=(
            "entityType must be exactly one of "
            '["ExtractedPerson"] or ["ExtractedContactPoint"]'
        ),
    ),
]
