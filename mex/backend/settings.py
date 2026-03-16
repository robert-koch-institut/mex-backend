from pydantic import AnyHttpUrl, Field, SecretStr

from mex.backend.types import APIKeyDatabase, MergedType, OIDCGroupsDatabase
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
    valkey_url: SecretStr | None = Field(
        None,
        description="Fully qualified URL of a valkey cache server.",
        validation_alias="MEX_BACKEND_VALKEY_URL",
    )
    oidc_issuer_url: AnyHttpUrl = Field(
        AnyHttpUrl("http://localhost:5556/dex"),
        description="OIDC issuer URL. JWKS fetched from {url}/.well-known/jwks.json.",
        validation_alias="MEX_OIDC_ISSUER_URL",
    )
    oidc_client_id: str = Field(
        "mex-backend",
        description="Expected 'aud' claim in OIDC JWTs (must match Dex client id).",
        validation_alias="MEX_OIDC_CLIENT_ID",
    )
    oidc_algorithms: list[str] = Field(
        ["RS256"],
        description="Allowed JWT signing algorithms.",
        validation_alias="MEX_OIDC_ALGORITHMS",
    )
    oidc_groups_database: OIDCGroupsDatabase = Field(
        OIDCGroupsDatabase(),
        description=(
            "Mapping of OIDC group names to access levels. "
            "Users in 'write' groups implicitly have read access too."
        ),
        validation_alias="MEX_BACKEND_OIDC_GROUPS_DATABASE",
    )
