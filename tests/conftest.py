import json
from base64 import b64encode
from functools import partial
from itertools import count
from typing import Any, cast
from unittest.mock import MagicMock, Mock

import pytest
from fastapi.testclient import TestClient
from neo4j import WRITE_ACCESS, Driver, Session, SummaryCounters, Transaction
from pytest import MonkeyPatch
from redis.client import Redis

from mex.artificial.helpers import generate_artificial_extracted_items
from mex.backend.cache.connector import CacheConnector, CacheProto, LocalCache
from mex.backend.graph.connector import GraphConnector
from mex.backend.identity.provider import GraphIdentityProvider
from mex.backend.main import app
from mex.backend.rules.helpers import create_and_get_rule_set
from mex.backend.settings import BackendSettings
from mex.backend.types import APIKeyDatabase, APIUserDatabase
from mex.common.connector import CONNECTOR_STORE
from mex.common.models import (
    MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
    AdditiveOrganizationalUnit,
    AnyExtractedModel,
    ExtractedActivity,
    ExtractedContactPoint,
    ExtractedOrganization,
    ExtractedOrganizationalUnit,
    ExtractedPrimarySource,
    OrganizationalUnitRuleSetRequest,
    OrganizationalUnitRuleSetResponse,
)
from mex.common.transform import MExEncoder
from mex.common.types import (
    Email,
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


@pytest.fixture(autouse=True)
def skip_integration_test_in_ci(is_integration_test: bool) -> None:  # noqa: FBT001
    """Overwrite fixture from plugin to not skip integration tests in ci."""


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
                    Mock(counters=SummaryCounters({})),
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
                        Mock(counters=SummaryCounters({})),
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
    session = MagicMock(spec=Session, run=run, begin_transaction=tx)
    session.__enter__ = MagicMock(spec=Session.__enter__, return_value=session)
    get_session = MagicMock(spec=Driver.session, return_value=session)
    driver = Mock(spec=Driver, session=get_session)
    monkeypatch.setattr(
        GraphConnector, "__init__", lambda self: setattr(self, "driver", driver)
    )
    return MockedGraph(run, session)


@pytest.fixture
def mocked_redis(monkeypatch: MonkeyPatch) -> CacheProto:
    """Mock the redis client to use a local cache instead."""
    cache = LocalCache()
    monkeypatch.setattr(Redis, "from_url", lambda _: cache)
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
def isolate_redis_cache(
    is_integration_test: bool,  # noqa: FBT001
    settings: BackendSettings,
    monkeypatch: MonkeyPatch,
) -> None:
    """Automatically flush the redis cache for integration testing."""
    if is_integration_test:
        monkeypatch.setattr(settings, "debug", True)
        connector = CacheConnector.get()
        connector.flush()
        connector.close()
        CONNECTOR_STORE.pop(CacheConnector)


def get_graph() -> list[dict[str, Any]]:
    connector = GraphConnector.get()
    graph = connector.commit(
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
    ).one()
    return sorted(
        graph["nodes"] + graph["relations"],
        key=lambda i: json.dumps(i, sort_keys=True),
    )


@pytest.fixture
def dummy_data(
    set_identity_provider: None,  # noqa: ARG001
) -> dict[str, AnyExtractedModel]:
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
    organization_1 = ExtractedOrganization(
        hadPrimarySource=primary_source_1.stableTargetId,
        identifierInPrimarySource="rki",
        officialName=[
            Text(value="RKI", language=TextLanguage.DE),
            Text(value="Robert Koch Institut ist the best", language=TextLanguage.DE),
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
    organizational_unit_1 = ExtractedOrganizationalUnit(
        hadPrimarySource=primary_source_2.stableTargetId,
        identifierInPrimarySource="ou-1",
        name=[Text(value="Unit 1", language=TextLanguage.EN)],
        unitOf=[organization_1.stableTargetId],
    )
    organizational_unit_2 = ExtractedOrganizationalUnit(
        hadPrimarySource=primary_source_2.stableTargetId,
        identifierInPrimarySource="ou-1.6",
        name=[Text(value="Unit 1.6", language=TextLanguage.EN)],
        parentUnit=organizational_unit_1.stableTargetId,
        unitOf=[organization_1.stableTargetId],
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
        start=[YearMonthDay("2014-08-24")],
        theme=[Theme["INFECTIOUS_DISEASES_AND_EPIDEMIOLOGY"]],
        title=[Text(value="Aktivität 1", language=TextLanguage.DE)],
        website=[Link(title="Activity Homepage", url="https://activity-1")],
    )
    return {
        "primary_source_1": primary_source_1,
        "primary_source_2": primary_source_2,
        "contact_point_1": contact_point_1,
        "contact_point_2": contact_point_2,
        "organization_1": organization_1,
        "organization_2": organization_2,
        "organizational_unit_1": organizational_unit_1,
        "organizational_unit_2": organizational_unit_2,
        "activity_1": activity_1,
    }


@pytest.fixture
def artificial_extracted_items() -> list[AnyExtractedModel]:
    return generate_artificial_extracted_items(
        locale="de_DE",
        seed=42,
        count=25,
        chattiness=16,
        # this order is important as it represents the direction of graph relations
        # TODO(ND): let's move that to mex-common and infer it programmatically
        stem_types=[
            "PrimarySource",
            "Organization",
            "OrganizationalUnit",
            "ContactPoint",
            "Person",
            "Consent",
            "AccessPlatform",
            "Distribution",
            "BibliographicResource",
            "Activity",
            "Resource",
            "VariableGroup",
            "Variable",
        ],
    )


def _match_organization_items(dummy_data: dict[str, AnyExtractedModel]) -> None:
    # TODO(ND): replace this crude item matching implementation (stopgap MX-1530)
    connector = GraphConnector.get()
    # remove the merged item for org2
    with connector.driver.session(default_access_mode=WRITE_ACCESS) as session:
        connector.commit(
            f"""\
    MATCH(n) WHERE n.identifier='{dummy_data["organization_2"].stableTargetId}'
    DETACH DELETE n;""",
            session=session,
        )
        # connect the extracted item for org2 with the merged item for org1
        connector.commit(
            f"""\
    MATCH(n :ExtractedOrganization) WHERE n.identifier = '{dummy_data["organization_2"].identifier}'
    MATCH(m :MergedOrganization) WHERE m.identifier = '{dummy_data["organization_1"].stableTargetId}'
    MERGE (n)-[:stableTargetId {{position:0}}]->(m);""",
            session=session,
        )
    # clear the identity provider cache to refresh the `stableTargetId` property on org2
    provider = GraphIdentityProvider.get()
    provider._cache.flush()


@pytest.fixture
def load_dummy_data(
    dummy_data: dict[str, AnyExtractedModel],
) -> dict[str, AnyExtractedModel]:
    """Ingest dummy data into the graph."""
    connector = GraphConnector.get()
    connector.ingest(list(dummy_data.values()))
    _match_organization_items(dummy_data)
    return dummy_data


@pytest.fixture
def load_artificial_extracted_items(
    artificial_extracted_items: list[AnyExtractedModel],
) -> list[AnyExtractedModel]:
    """Ingest artificial data into the graph."""
    GraphConnector.get().ingest(artificial_extracted_items)
    return artificial_extracted_items


@pytest.fixture
def additive_organizational_unit(
    dummy_data: dict[str, AnyExtractedModel],
) -> AdditiveOrganizationalUnit:
    return AdditiveOrganizationalUnit(
        name=[Text(value="Unit 1.7", language=TextLanguage.EN)],
        website=[Link(title="Unit Homepage", url="https://unit-1-7")],
        parentUnit=dummy_data["organizational_unit_1"].stableTargetId,
    )


@pytest.fixture
def organizational_unit_rule_set_request(
    additive_organizational_unit: AdditiveOrganizationalUnit,
) -> OrganizationalUnitRuleSetRequest:
    return OrganizationalUnitRuleSetRequest(additive=additive_organizational_unit)


@pytest.fixture
def organizational_unit_rule_set_response(
    additive_organizational_unit: AdditiveOrganizationalUnit,
) -> OrganizationalUnitRuleSetResponse:
    return OrganizationalUnitRuleSetResponse(
        additive=additive_organizational_unit, stableTargetId=Identifier.generate(42)
    )


@pytest.fixture
def load_dummy_rule_set(
    organizational_unit_rule_set_request: OrganizationalUnitRuleSetRequest,
    load_dummy_data: dict[str, AnyExtractedModel],
) -> OrganizationalUnitRuleSetResponse:
    connector = GraphConnector.get()
    connector.ingest(
        [
            load_dummy_data["primary_source_2"],
            load_dummy_data["organizational_unit_1"],
            load_dummy_data["organizational_unit_2"],
        ]
    )
    return cast(
        "OrganizationalUnitRuleSetResponse",
        create_and_get_rule_set(
            organizational_unit_rule_set_request,
            stable_target_id=load_dummy_data["organizational_unit_2"].stableTargetId,
        ),
    )
