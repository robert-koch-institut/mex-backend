from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

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
