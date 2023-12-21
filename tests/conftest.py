import json
from functools import partial
from unittest.mock import MagicMock, Mock

import pytest
from fastapi.testclient import TestClient
from neo4j import GraphDatabase
from pytest import MonkeyPatch

from mex.backend.extracted.models import EXTRACTED_MODEL_CLASSES_BY_NAME
from mex.backend.graph.connector import GraphConnector
from mex.backend.main import app
from mex.backend.settings import BackendSettings
from mex.backend.types import APIKeyDatabase
from mex.common.models import BaseExtractedData
from mex.common.transform import MExEncoder
from mex.common.types import Identifier, Link, Text, TextLanguage

pytest_plugins = ("mex.common.testing.plugin",)


@pytest.fixture(autouse=True)
def settings() -> BackendSettings:
    """Load the settings for this pytest session."""
    settings = BackendSettings.get()
    settings.backend_api_key_database = APIKeyDatabase(
        **{"read": "read_key", "write": "write_key"}
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
def client_with_write_permission(client: TestClient) -> TestClient:
    """Return a fastAPI test client with write permission initialized with our app."""
    client.headers.update({"X-API-Key": "write_key"})
    return client


@pytest.fixture
def client_with_read_permission(client: TestClient) -> TestClient:
    """Return a fastAPI test client with read permission initialized with our app."""
    client.headers.update({"X-API-Key": "read_key"})
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


@pytest.fixture
def extracted_person() -> BaseExtractedData:
    """Return an extracted person with static dummy values."""
    return EXTRACTED_MODEL_CLASSES_BY_NAME["Person"](
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
def load_dummy_data() -> None:
    """Ingest dummy data into Graph Database."""
    GraphConnector.get().ingest(
        [
            EXTRACTED_MODEL_CLASSES_BY_NAME["PrimarySource"](
                hadPrimarySource="psSti00000000001",
                identifier="psId000000000001",
                identifierInPrimarySource="ps-1",
                stableTargetId="psSti00000000001",
            ),
            EXTRACTED_MODEL_CLASSES_BY_NAME["PrimarySource"](
                hadPrimarySource="psSti00000000001",
                identifier="psId000000000002",
                identifierInPrimarySource="ps-2",
                stableTargetId="psSti00000000002",
                title=[Text(value="A cool and searchable title", language=None)],
            ),
            EXTRACTED_MODEL_CLASSES_BY_NAME["ContactPoint"](
                email="info@rki.de",
                hadPrimarySource="psSti00000000001",
                identifier="cpId000000000001",
                identifierInPrimarySource="cp-1",
                stableTargetId="cpSti00000000001",
            ),
            EXTRACTED_MODEL_CLASSES_BY_NAME["ContactPoint"](
                email="mex@rki.de",
                hadPrimarySource="psSti00000000001",
                identifier="cpId000000000002",
                identifierInPrimarySource="cp-2",
                stableTargetId="cpSti00000000002",
            ),
            EXTRACTED_MODEL_CLASSES_BY_NAME["OrganizationalUnit"](
                hadPrimarySource="psSti00000000002",
                identifier="ouId000000000001",
                identifierInPrimarySource="ou-1",
                name="Unit 1",
                stableTargetId="ouSti00000000001",
            ),
            EXTRACTED_MODEL_CLASSES_BY_NAME["OrganizationalUnit"](
                hadPrimarySource="psSti00000000001",
                identifier="ouId000000000002",
                identifierInPrimarySource="ou-2",
                name="Unit 2",
                stableTargetId="ouSti00000000001",
            ),
            EXTRACTED_MODEL_CLASSES_BY_NAME["Activity"](
                abstract=[
                    Text(value="An active activity.", language=TextLanguage.EN),
                    Text(value="Mumble bumble boo.", language=None),
                ],
                contact=["cpSti00000000001", "cpSti00000000002", "ouSti00000000001"],
                hadPrimarySource="psSti00000000001",
                identifier="aId0000000000001",
                identifierInPrimarySource="a-1",
                responsibleUnit=["ouSti00000000001"],
                stableTargetId="aSti000000000001",
                theme=["https://mex.rki.de/item/theme-3"],
                title=["Activity 1"],
                website=[Link(title="Activity Homepage", url="https://activity-1")],
            ),
        ]
    )
