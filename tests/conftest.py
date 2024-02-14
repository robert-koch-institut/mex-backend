import json
from base64 import b64encode
from functools import partial
from itertools import count
from typing import Any
from unittest.mock import MagicMock, Mock

import pytest
from fastapi.testclient import TestClient
from neo4j import GraphDatabase, SummaryCounters
from pytest import MonkeyPatch

from mex.backend.graph.connector import GraphConnector
from mex.backend.main import app
from mex.backend.settings import BackendSettings
from mex.backend.types import APIKeyDatabase, APIUserDatabase
from mex.common.models import (
    MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
    AnyExtractedModel,
    ExtractedActivity,
    ExtractedContactPoint,
    ExtractedOrganizationalUnit,
    ExtractedPrimarySource,
)
from mex.common.transform import MExEncoder
from mex.common.types import (
    Email,
    Identifier,
    Link,
    Text,
    TextLanguage,
    Theme,
)
from mex.common.types.identifier import IdentifierT

pytest_plugins = ("mex.common.testing.plugin",)


@pytest.fixture(autouse=True)
def settings() -> BackendSettings:
    """Load the settings for this pytest session."""
    settings = BackendSettings.get()
    settings.backend_api_key_database = APIKeyDatabase(
        **{"read": "read_key", "write": "write_key"}
    )
    settings.backend_user_database = APIUserDatabase(
        **{"read": {"Reader": "read_password"}, "write": {"Writer": "write_password"}}
    )
    return settings


@pytest.fixture(autouse=True)
def skip_integration_test_in_ci(is_integration_test: bool) -> None:
    """Overwrite fixture from plugin to not skip integration tests in ci."""


@pytest.fixture
def client() -> TestClient:
    """Return a fastAPI test client initialized with our app."""
    return TestClient(app)


@pytest.fixture
def client_with_api_key_write_permission(client: TestClient) -> TestClient:
    """Return a fastAPI test client with write permission initialized with our app."""
    client.headers.update({"X-API-Key": "write_key"})
    return client


@pytest.fixture
def client_with_api_key_read_permission(client: TestClient) -> TestClient:
    """Return a fastAPI test client with read permission granted by API key."""
    client.headers.update({"X-API-Key": "read_key"})
    return client


@pytest.fixture
def client_with_basic_auth_read_permission(client: TestClient) -> TestClient:
    """Return a fastAPI test client with read permission granted by basic auth."""
    client.headers.update(
        {"Authorization": f"Basic {b64encode(b'Reader:read_password').decode()}"}
    )
    return client


@pytest.fixture
def client_with_basic_auth_write_permission(client: TestClient) -> TestClient:
    """Return a fastAPI test client with write permission granted by basic auth."""
    client.headers.update(
        {"Authorization": f"Basic {b64encode(b'Writer:write_password').decode()}"}
    )
    return client


@pytest.fixture(autouse=True)
def patch_test_client_json_encoder(monkeypatch: MonkeyPatch) -> None:
    """Automatically allow the test client to encode our models to JSON."""
    monkeypatch.setattr(
        "httpx._content.json_dumps", partial(json.dumps, cls=MExEncoder)
    )


class MockedGraph:
    def __init__(self, records: list[Any], session_run: MagicMock) -> None:
        self.records = records
        self.run = session_run

    @property
    def return_value(self) -> list[Any]:
        return self.records

    @return_value.setter
    def return_value(self, value: list[Any]) -> None:
        self.records[:] = [Mock(data=MagicMock(return_value=v)) for v in value]

    @property
    def call_args_list(self) -> list[Any]:
        return self.run.call_args_list


@pytest.fixture
def mocked_graph(monkeypatch: MonkeyPatch) -> MockedGraph:
    """Mock the graph connector and return the mocked `run` for easy manipulation."""
    records: list[Any] = []
    summary = Mock(counters=SummaryCounters({}))
    result = Mock(to_eager_result=MagicMock(return_value=(records, summary, None)))
    run = MagicMock(return_value=result)
    session = MagicMock(__enter__=MagicMock(return_value=Mock(run=run)))
    driver = Mock(session=MagicMock(return_value=session))
    monkeypatch.setattr(
        GraphConnector, "__init__", lambda self: setattr(self, "driver", driver)
    )
    return MockedGraph(records, run)


@pytest.fixture(autouse=True)
def isolate_identifier_seeds(monkeypatch: MonkeyPatch) -> None:
    """Ensure the identifier class produces deterministic IDs."""
    counter = count()
    original_generate = Identifier.generate

    def generate(cls: type[IdentifierT], seed: int | None = None) -> IdentifierT:
        return cls(original_generate(seed or next(counter)))

    monkeypatch.setattr(Identifier, "generate", classmethod(generate))


@pytest.fixture(autouse=True)
def isolate_graph_database(
    is_integration_test: bool, settings: BackendSettings
) -> None:
    """Automatically flush the neo4j database for integration testing."""
    if is_integration_test:  # pragma: no cover
        with GraphDatabase.driver(
            settings.graph_url,
            auth=(
                settings.graph_user.get_secret_value(),
                settings.graph_password.get_secret_value(),
            ),
            database=settings.graph_db,
        ) as driver:
            driver.execute_query("MATCH (n) DETACH DELETE n;")
            for row in driver.execute_query("SHOW ALL CONSTRAINTS;").records:
                driver.execute_query(f"DROP CONSTRAINT {row['name']};")
            for row in driver.execute_query("SHOW ALL INDEXES;").records:
                driver.execute_query(f"DROP INDEX {row['name']};")


@pytest.fixture
def dummy_data() -> list[AnyExtractedModel]:
    """Create a set of interlinked dummy data."""
    primary_source_1 = ExtractedPrimarySource(
        hadPrimarySource=MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
        identifierInPrimarySource="ps-1",
    )
    primary_source_2 = ExtractedPrimarySource(
        hadPrimarySource=MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
        identifierInPrimarySource="ps-2",
        version="Cool Version v2.13",
    )
    contact_point_1 = ExtractedContactPoint(
        email=[Email("info@contact-point.one")],
        hadPrimarySource=primary_source_1.stableTargetId,
        identifierInPrimarySource="cp-1",
    )
    contact_point_2 = ExtractedContactPoint(
        email=[Email("help@contact-point.two")],
        hadPrimarySource=primary_source_1.stableTargetId,
        identifierInPrimarySource="cp-2",
    )
    organizational_unit_1 = ExtractedOrganizationalUnit(
        hadPrimarySource=primary_source_2.stableTargetId,
        identifierInPrimarySource="ou-1",
        name=[Text(value="Unit 1", language=TextLanguage.EN)],
    )
    activity_1 = ExtractedActivity(
        abstract=[
            Text(value="An active activity.", language=TextLanguage.EN),
            Text(value="Une activité active.", language=None),
        ],
        contact=[
            contact_point_1.stableTargetId,
            contact_point_2.stableTargetId,
            organizational_unit_1.stableTargetId,
        ],
        hadPrimarySource=primary_source_1.stableTargetId,
        identifierInPrimarySource="a-1",
        responsibleUnit=[organizational_unit_1.stableTargetId],
        theme=[Theme["DIGITAL_PUBLIC_HEALTH"]],
        title=[Text(value="Aktivität 1", language=TextLanguage.DE)],
        website=[Link(title="Activity Homepage", url="https://activity-1")],
    )
    return [
        primary_source_1,
        primary_source_2,
        contact_point_1,
        contact_point_2,
        organizational_unit_1,
        activity_1,
    ]


@pytest.fixture
def load_dummy_data(dummy_data: list[AnyExtractedModel]) -> list[AnyExtractedModel]:
    """Ingest dummy data into the graph."""
    GraphConnector.get().ingest(dummy_data)
    return dummy_data
