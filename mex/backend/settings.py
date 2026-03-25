from typing import Self

from pydantic import Field, SecretStr, model_validator

from mex.backend.models import APIKeyDatabase, APIUserDatabase
from mex.backend.types import MergedType
from mex.common.settings import BaseSettings


class BackendSettings(BaseSettings):
    """Settings definition for the backend server."""

    backend_host: str = Field(
        "localhost",
        min_length=1,
        max_length=250,
        description="Host that the backend server will run on.",
        validation_alias="MEX_BACKEND_HOST",
    )
    backend_port: int = Field(
        8080,
        gt=0,
        lt=65536,
        description="Port that the backend server should listen on.",
        validation_alias="MEX_BACKEND_PORT",
    )
    backend_root_path: str = Field(
        "",
        description="Root path that the backend server should run under.",
        validation_alias="MEX_BACKEND_ROOT_PATH",
    )
    graph_url: str = Field(
        "neo4j://localhost:7687",
        description="URL for connecting to the graph database.",
        validation_alias="MEX_GRAPH_URL",
    )
    graph_db: str = Field(
        "neo4j",
        description="Name of the default graph database.",
        validation_alias="MEX_GRAPH_NAME",
    )
    graph_user: SecretStr = Field(
        SecretStr("neo4j"),
        description="Username for authenticating with the graph database.",
        validation_alias="MEX_GRAPH_USER",
    )
    graph_password: SecretStr = Field(
        SecretStr("password"),
        description="Password for authenticating with the graph database.",
        validation_alias="MEX_GRAPH_PASSWORD",
    )
    graph_tx_timeout: int | float = Field(
        15.0,
        description=(
            "The graph transaction timeout in seconds. "
            "A 0 duration will make the transaction execute indefinitely. "
            "None will use the default timeout configured on the server."
        ),
        validation_alias="MEX_GRAPH_TX_TIMEOUT",
    )
    graph_session_timeout: int | float = Field(
        45.0,
        description=(
            "Maximum time transactions are allowed to retry via tx functions."
        ),
        validation_alias="MEX_GRAPH_SESSION_TIMEOUT",
    )
    non_matchable_types: list[MergedType] = Field(
        [MergedType("MergedConsent"), MergedType("MergedPerson")],
        description="Block item matching for merged items with these entity types.",
        validation_alias="MEX_BACKEND_NON_MATCHABLE_TYPES",
    )
    backend_api_key_database: APIKeyDatabase = Field(
        APIKeyDatabase(),
        description="Database of API keys.",
        validation_alias="MEX_BACKEND_API_KEY_DATABASE",
    )
    backend_user_database: APIUserDatabase = Field(
        APIUserDatabase(),
        description="Database of users.",
        validation_alias="MEX_BACKEND_API_USER_DATABASE",
    )
    valkey_url: SecretStr | None = Field(
        None,
        description="Fully qualified URL of a valkey cache server.",
        validation_alias="MEX_BACKEND_VALKEY_URL",
    )

    @model_validator(mode="after")
    def assert_valkey_is_configured_when_parallelized(self) -> Self:
        """Validate that valkey is configured if parallelization is > 1.

        Rationale: We cache identities to make sure that multiple calls for getting an
        identifier receive the same identifier, even if the item with this identifier
        is not yet ingested in our database. If we use multiple backend instances, they
        must use a shared cache for storing these identities. The only shared cache is
        valkey, hence we make sure that valkey is configured if parallelization > 1.
        """
        if self.backend_api_parallelization > 1 and self.valkey_url is None:
            msg = "If parallelization is > 1, valkey url must be set."
            raise ValueError(msg)
        return self
