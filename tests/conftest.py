import json
from base64 import b64encode
from functools import partial
from itertools import count
from unittest.mock import MagicMock, Mock

import pytest
from fastapi.testclient import TestClient
from neo4j import GraphDatabase
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
    ExtractedPerson,
    ExtractedPrimarySource,
)
from mex.common.transform import MExEncoder
from mex.common.types import Identifier, Link, Text, TextLanguage
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
    """Overwrite fixture from plugin to not skip int tests in ci."""


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


@pytest.fixture
def mocked_graph(monkeypatch: MonkeyPatch) -> MagicMock:
    """Mock the graph connector and return the mocked `run` for easy manipulation."""
    data = MagicMock(return_value=[])
    run = MagicMock(return_value=Mock(data=data))
    data.run = run  # make the call_args available in tests
    session = MagicMock(__enter__=MagicMock(return_value=Mock(run=run)))
    driver = Mock(session=MagicMock(return_value=session))
    monkeypatch.setattr(GraphDatabase, "driver", lambda _, **__: driver)
    return data


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
            driver.execute_query("DROP INDEX text_fields IF EXISTS;")
            driver.execute_query("DROP CONSTRAINT identifier_uniqueness IF EXISTS;")


@pytest.fixture
def extracted_person() -> ExtractedPerson:
    """Return an extracted person with static dummy values."""
    return ExtractedPerson.model_construct(
        identifier=Identifier.generate(seed=6),
        stableTargetId=Identifier.generate(seed=66),
        affiliation=[
            Identifier.generate(seed=255),
            Identifier.generate(seed=3810),
        ],
        email=["fictitiousf@rki.de", "info@rki.de"],
        familyName=["Fictitious"],
        givenName=["Frieda"],
        identifierInPrimarySource="frieda",
        fullName=["Fictitious, Frieda, Dr."],
        hadPrimarySource=Identifier.generate(seed=64),
        memberOf=[
            Identifier.generate(seed=35),
        ],
    )


@pytest.fixture
def load_dummy_data() -> list[AnyExtractedModel]:
    """Ingest dummy data into Graph Database."""
    primary_source_1 = ExtractedPrimarySource(
        hadPrimarySource=MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
        identifierInPrimarySource="ps-1",
    )
    primary_source_2 = ExtractedPrimarySource(
        hadPrimarySource=MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
        identifierInPrimarySource="ps-2",
        title=[Text(value="A cool and searchable title", language=None)],
    )
    contact_point_1 = ExtractedContactPoint(
        email="info@rki.de",
        hadPrimarySource=primary_source_1.stableTargetId,
        identifierInPrimarySource="cp-1",
    )
    contact_point_2 = ExtractedContactPoint(
        email="mex@rki.de",
        hadPrimarySource=primary_source_1.stableTargetId,
        identifierInPrimarySource="cp-2",
    )
    organizational_unit_1 = ExtractedOrganizationalUnit(
        hadPrimarySource=primary_source_2.stableTargetId,
        identifierInPrimarySource="ou-1",
        name="Unit 1",
    )
    activity_1 = ExtractedActivity(
        abstract=[
            Text(value="An active activity.", language=TextLanguage.EN),
            Text(value="Mumble bumble boo.", language=None),
        ],
        contact=[
            contact_point_1.stableTargetId,
            contact_point_2.stableTargetId,
            organizational_unit_1.stableTargetId,
        ],
        hadPrimarySource=primary_source_1.stableTargetId,
        identifierInPrimarySource="a-1",
        responsibleUnit=[organizational_unit_1.stableTargetId],
        theme=["https://mex.rki.de/item/theme-3"],
        title=["Activity 1"],
        website=[Link(title="Activity Homepage", url="https://activity-1")],
    )
    models = [
        primary_source_1,
        primary_source_2,
        contact_point_1,
        contact_point_2,
        organizational_unit_1,
        activity_1,
    ]
    GraphConnector.get().ingest(models)
    return models
