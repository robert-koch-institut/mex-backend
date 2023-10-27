from pydantic import Field, SecretStr

from mex.backend.types import UserDatabase
from mex.common.settings import BaseSettings
from mex.common.sinks import Sink


class BackendSettings(BaseSettings):
    """Settings definition for the backend server."""

    debug: bool = Field(
        False, alias="reload", description="Enable debug mode.", env="MEX_DEBUG"
    )
    sink: list[Sink] = Field(
        [Sink.GRAPH],
        description=(
            "Where to send ingested data. Defaults to writing to the graph db."
        ),
        env="MEX_SINK",
    )
    backend_host: str = Field(
        "localhost",
        min_length=1,
        max_length=250,
        description="Host that the backend server will run on.",
        env="MEX_BACKEND_HOST",
    )
    backend_port: int = Field(
        8080,
        gt=0,
        lt=65536,
        description="Port that the backend server should listen on.",
        env="MEX_BACKEND_PORT",
    )
    backend_root_path: str = Field(
        "",
        description="Root path that the backend server should run under.",
        env="MEX_BACKEND_ROOT_PATH",
    )
    graph_url: str = Field(
        "neo4j://localhost:7687",
        description="URL of the neo4j HTTP API endpoint including the graph name.",
        env="MEX_GRAPH_URL",
    )
    graph_db: str = Field(
        "neo4j",
        description="Name of the neo4j graph database.",
        env="MEX_GRAPH_NAME",
    )
    graph_user: SecretStr = Field(
        SecretStr("neo4j"),
        description="Username for authenticating with the neo4j graph.",
        env="MEX_GRAPH_USER",
    )
    graph_password: SecretStr = Field(
        SecretStr("password"),
        description="Password for authenticating with the neo4j graph.",
        env="MEX_GRAPH_PASSWORD",
    )
    backend_user_database: UserDatabase = Field(
        UserDatabase(),
        description="Database of users.",
        env="MEX_BACKEND_USER_DATABASE",
    )
