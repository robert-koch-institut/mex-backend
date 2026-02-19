import json
from base64 import b64encode
from collections import deque
from functools import partial
from itertools import count
from typing import Any, Literal, TypedDict
from unittest.mock import MagicMock, Mock

import pytest
from fastapi.testclient import TestClient
from neo4j import Driver, Session, SummaryCounters, Transaction
from pytest import FixtureRequest, MonkeyPatch
from valkey import Valkey

from mex.artificial.helpers import create_artificial_items_and_rule_sets
from mex.backend.cache.connector import CacheConnector, LocalCache, ValkeyCache
from mex.backend.graph.connector import GraphConnector
from mex.backend.main import app
from mex.backend.settings import BackendSettings
from mex.backend.testing.main import app as testing_app
from mex.backend.types import APIKeyDatabase, APIUserDatabase
from mex.common.connector import CONNECTOR_STORE
from mex.common.models import (
    MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
    AdditiveOrganizationalUnit,
    AnyExtractedModel,
    AnyRuleSetResponse,
    ExtractedActivity,
    ExtractedContactPoint,
    ExtractedOrganization,
    ExtractedOrganizationalUnit,
    ExtractedPrimarySource,
    OrganizationalUnitRuleSetResponse,
    SubtractiveOrganizationalUnit,
)
from mex.common.transform import MExEncoder
from mex.common.types import (
    Identifier,
    IdentityProvider,
    Link,
    Text,
    TextLanguage,
    Theme,
    YearMonthDay,
)

pytest_plugins = ("mex.common.testing.plugin",)


@pytest.fixture(autouse=True)
def settings() -> BackendSettings:
    """Load the settings for this pytest session."""
    settings = BackendSettings.get()
    settings.backend_api_key_database = APIKeyDatabase(
        read="read_key", write="write_key"
    )
    settings.backend_user_database = APIUserDatabase(
        read={"Reader": "read_password"}, write={"Writer": "write_password"}
    )
    return settings


@pytest.fixture
def client() -> TestClient:
    """Return a fastAPI test client initialized with our app."""
    with TestClient(app, raise_server_exceptions=False) as test_client:
        return test_client


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


@pytest.fixture(params=["main", "testing"])
def entrypoint_app(request: FixtureRequest) -> TestClient:
    """Parametrized fixture to run tests against main or testing app."""
    if request.param == "testing":
        with TestClient(testing_app, raise_server_exceptions=False) as test_client:
            return test_client
    with TestClient(app, raise_server_exceptions=False) as test_client:
        return test_client


class MockedGraph:
    def __init__(self, run: MagicMock, session: MagicMock) -> None:
        self.run = run
        self.session = session
        self.return_value = []

    @property
    def return_value(self) -> list[Any]:  # pragma: no cover
        raise NotImplementedError

    @return_value.setter
    def return_value(self, value: list[dict[str, Any]]) -> None:
        self.run.return_value = Mock(
            to_eager_result=MagicMock(
                return_value=(
                    [Mock(items=v.items) for v in value],
                    Mock(counters=SummaryCounters({}), gql_status_objects=[]),
                    None,
                ),
            ),
        )

    @property
    def side_effect(self) -> list[Any]:  # pragma: no cover
        raise NotImplementedError

    @side_effect.setter
    def side_effect(self, values: list[list[dict[str, Any]]]) -> None:
        self.run.side_effect = [
            Mock(
                to_eager_result=MagicMock(
                    return_value=(
                        [Mock(items=v.items) for v in value],
                        Mock(counters=SummaryCounters({}), gql_status_objects=[]),
                        None,
                    ),
                ),
            )
            for value in values
        ]

    @property
    def call_args_list(self) -> list[Any]:
        return self.run.call_args_list


@pytest.fixture
def mocked_graph(monkeypatch: MonkeyPatch) -> MockedGraph:
    """Mock the graph connector and return the mocked `run` for easy manipulation."""
    run = MagicMock(spec=Session.run)
    tx = MagicMock(spec=Transaction, run=run)
    tx.__enter__ = MagicMock(spec=Transaction.__enter__, return_value=tx)
    begin_transaction = MagicMock(spec=Session.begin_transaction, return_value=tx)
    session = MagicMock(spec=Session, run=run, begin_transaction=begin_transaction)
    session.__enter__ = MagicMock(spec=Session.__enter__, return_value=session)
    begin_session = MagicMock(spec=Driver.session, return_value=session)
    driver = Mock(spec=Driver, session=begin_session)
    monkeypatch.setattr(
        GraphConnector, "__init__", lambda self: setattr(self, "driver", driver)
    )
    return MockedGraph(run, session)


@pytest.fixture
def mocked_valkey(monkeypatch: MonkeyPatch) -> LocalCache | ValkeyCache:
    """Mock the valkey client to use a local cache instead."""
    cache = LocalCache()
    monkeypatch.setattr(Valkey, "from_url", lambda _: cache)
    return cache


@pytest.fixture(autouse=True)
def isolate_identifier_seeds(monkeypatch: MonkeyPatch) -> None:
    """Ensure the identifier class produces deterministic IDs."""
    counter = count()
    original_generate = Identifier.generate

    def generate(cls: type[Identifier], seed: int | None = None) -> Identifier:
        return cls(original_generate(seed or next(counter)))

    monkeypatch.setattr(Identifier, "generate", classmethod(generate))


@pytest.fixture(autouse=True)
def set_identity_provider(
    is_integration_test: bool,  # noqa: FBT001
    monkeypatch: MonkeyPatch,
) -> None:
    """Ensure the identifier provider is set correctly for unit and int tests."""
    # TODO(ND): yuck, all this needs cleaning up after MX-1596
    settings = BackendSettings.get()
    if is_integration_test:
        monkeypatch.setitem(settings.model_config, "validate_assignment", False)  # noqa: FBT003
        monkeypatch.setattr(settings, "identity_provider", IdentityProvider.GRAPH)
    else:
        monkeypatch.setattr(settings, "identity_provider", IdentityProvider.MEMORY)


@pytest.fixture(autouse=True)
def isolate_graph_database(
    is_integration_test: bool,  # noqa: FBT001
    settings: BackendSettings,
    monkeypatch: MonkeyPatch,
) -> None:
    """Automatically flush the graph database for integration testing."""
    if is_integration_test:
        monkeypatch.setattr(settings, "debug", True)
        connector = GraphConnector.get()
        connector.flush()
        connector.close()
        CONNECTOR_STORE.pop(GraphConnector)


@pytest.fixture(autouse=True)
def isolate_valkey_cache(
    is_integration_test: bool,  # noqa: FBT001
    settings: BackendSettings,
    monkeypatch: MonkeyPatch,
) -> None:
    """Automatically flush the valkey cache for integration testing."""
    if is_integration_test:
        monkeypatch.setattr(settings, "debug", True)
        connector = CacheConnector.get()
        connector.flush()
        connector.close()
        CONNECTOR_STORE.pop(CacheConnector)


def get_graph() -> list[dict[str, Any]]:
    connector = GraphConnector.get()
    result = connector.driver.execute_query(
        """
CALL () {
    MATCH (n)
    RETURN collect(n{
        .*, label: head(labels(n))
    }) AS nodes
}
CALL () {
    MATCH ()-[r]->()
    RETURN collect({
        label: type(r), position: r.position,
        start: coalesce(startNode(r).identifier, head(labels(startNode(r)))),
        end: coalesce(endNode(r).identifier, head(labels(endNode(r))))
    }) as relations
}
RETURN nodes, relations;
"""
    )
    return sorted(
        result.records[0]["nodes"] + result.records[0]["relations"],
        key=lambda i: json.dumps(i, sort_keys=True),
    )


class DummyData(TypedDict):
    primary_source_1: ExtractedPrimarySource
    primary_source_2: ExtractedPrimarySource
    contact_point_1: ExtractedContactPoint
    contact_point_2: ExtractedContactPoint
    organization_1: ExtractedOrganization
    organization_2: ExtractedOrganization
    unit_1: ExtractedOrganizationalUnit
    unit_2: ExtractedOrganizationalUnit
    unit_2_rule_set: OrganizationalUnitRuleSetResponse
    unit_3_standalone_rule_set: OrganizationalUnitRuleSetResponse
    activity_1: ExtractedActivity


DummyDataName = Literal[
    "primary_source_1",
    "primary_source_2",
    "contact_point_1",
    "contact_point_2",
    "organization_1",
    "organization_2",
    "unit_1",
    "unit_2",
    "unit_2_rule_set",
    "unit_3_standalone_rule_set",
    "activity_1",
]


@pytest.fixture
def dummy_data(
    set_identity_provider: None,  # noqa: ARG001
) -> DummyData:
    """Create interlinked dummy data and return a lookup by memorable names."""
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
        email=["info@contact-point.one"],
        hadPrimarySource=primary_source_1.stableTargetId,
        identifierInPrimarySource="cp-1",
    )
    contact_point_2 = ExtractedContactPoint(
        email=["help@contact-point.two"],
        hadPrimarySource=primary_source_1.stableTargetId,
        identifierInPrimarySource="cp-2",
    )
    organization_1 = ExtractedOrganization(
        hadPrimarySource=primary_source_1.stableTargetId,
        identifierInPrimarySource="rki",
        officialName=[
            Text(value="RKI", language=TextLanguage.DE),
        ],
    )
    organization_2 = ExtractedOrganization(
        hadPrimarySource=primary_source_2.stableTargetId,
        identifierInPrimarySource="robert-koch-institute",
        officialName=[
            Text(value="RKI", language=TextLanguage.DE),
            Text(value="Robert Koch Institute", language=TextLanguage.EN),
        ],
    )
    unit_1 = ExtractedOrganizationalUnit(
        hadPrimarySource=primary_source_2.stableTargetId,
        identifierInPrimarySource="ou-1",
        name=[Text(value="Unit 1", language=TextLanguage.EN)],
        unitOf=[organization_1.stableTargetId],
        website=[Link(url="https://ou-1")],
    )
    unit_2 = ExtractedOrganizationalUnit(
        hadPrimarySource=primary_source_2.stableTargetId,
        identifierInPrimarySource="ou-1.6",
        name=[Text(value="Unit 1.6", language=TextLanguage.EN)],
        unitOf=[organization_1.stableTargetId],
    )
    unit_2_rule_set = OrganizationalUnitRuleSetResponse(
        additive=AdditiveOrganizationalUnit(
            name=[Text(value="Abteilung 1.6", language=TextLanguage.DE)],
            website=[Link(title="Unit Homepage", url="https://unit-1-6")],
            parentUnit=unit_1.stableTargetId,
        ),
        subtractive=SubtractiveOrganizationalUnit(
            name=[Text(value="Unit 1.6", language=TextLanguage.EN)],
        ),
        stableTargetId=unit_2.stableTargetId,
    )
    unit_3_standalone_rule_set = OrganizationalUnitRuleSetResponse(
        additive=AdditiveOrganizationalUnit(
            name=[Text(value="Abteilung 1.7", language=TextLanguage.DE)],
            parentUnit=unit_1.stableTargetId,
            email="1.7@rki.de",
        ),
        stableTargetId=Identifier("StandaloneRule"),
    )
    activity_1 = ExtractedActivity(
        abstract=[
            Text(value="An active activity.", language=TextLanguage.EN),
            Text(value="Eng aktiv Aktivitéit.", language=None),
        ],
        contact=[
            contact_point_1.stableTargetId,
            contact_point_2.stableTargetId,
            unit_1.stableTargetId,
        ],
        hadPrimarySource=primary_source_1.stableTargetId,
        identifierInPrimarySource="a-1",
        responsibleUnit=[unit_1.stableTargetId],
        start=[YearMonthDay("2014-08-24")],
        theme=[Theme["INFECTIOUS_DISEASES_AND_EPIDEMIOLOGY"]],
        title=[Text(value="Aktivität 1", language=TextLanguage.DE)],
        website=[Link(title="Activity Homepage", url="https://activity-1")],
    )
    return DummyData(
        primary_source_1=primary_source_1,
        primary_source_2=primary_source_2,
        contact_point_1=contact_point_1,
        contact_point_2=contact_point_2,
        organization_1=organization_1,
        organization_2=organization_2,
        unit_1=unit_1,
        unit_2=unit_2,
        unit_2_rule_set=unit_2_rule_set,
        unit_3_standalone_rule_set=unit_3_standalone_rule_set,
        activity_1=activity_1,
    )


@pytest.fixture
def loaded_dummy_data(dummy_data: DummyData) -> DummyData:
    """Ingest dummy data into the graph."""
    connector = GraphConnector.get()
    deque(connector.ingest_items(dummy_data.values()))  # type: ignore[arg-type]
    return dummy_data


@pytest.fixture
def artificial_data() -> list[AnyExtractedModel | AnyRuleSetResponse]:
    """Return artificial dummy data."""
    return [
        item
        for container in create_artificial_items_and_rule_sets(
            locale="de_DE",
            seed=1,
            count=42,
            chattiness=8,
        )
        for item in [container.extracted_item, container.rule_set]
        if item
    ]


@pytest.fixture
def loaded_artificial_data(
    artificial_data: list[AnyExtractedModel | AnyRuleSetResponse],
) -> list[AnyExtractedModel | AnyRuleSetResponse]:
    """Ingest artificial data into the graph."""
    connector = GraphConnector.get()
    deque(connector.ingest_items(artificial_data))
    return artificial_data
