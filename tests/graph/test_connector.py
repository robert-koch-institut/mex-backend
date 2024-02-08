from unittest.mock import MagicMock

import pytest

from mex.backend.graph import queries as q
from mex.backend.graph.connector import GraphConnector
from mex.common.exceptions import MExError
from mex.common.models import AnyExtractedModel
from mex.common.types import Identifier, PrimarySourceID


@pytest.mark.usefixtures("mocked_graph")
def test_mocked_graph_init() -> None:
    graph = GraphConnector.get()
    result = graph.commit("MATCH (this);")

    assert result.all() == []


def test_mocked_graph_seed_constraints(mocked_graph: MagicMock) -> None:
    graph = GraphConnector.get()
    graph._seed_constraints()

    assert mocked_graph.call_args_list[-1].args == (
        """CREATE CONSTRAINT extracted_variable_group_identifier_uniqueness IF NOT EXISTS
FOR (n:ExtractedVariableGroup)
REQUIRE n.identifier IS UNIQUE;""",
        {},
    )


def test_mocked_graph_seed_indices(mocked_graph: MagicMock) -> None:
    graph = GraphConnector.get()
    graph._seed_indices()

    assert mocked_graph.call_args_list[-1].args == (
        """CREATE FULLTEXT INDEX search_index IF NOT EXISTS
FOR (n:ExtractedAccessPlatform|ExtractedActivity|ExtractedContactPoint|ExtractedDistribution|ExtractedOrganization|ExtractedOrganizationalUnit|ExtractedPerson|ExtractedPrimarySource|ExtractedResource|ExtractedVariable|ExtractedVariableGroup)
ON EACH [n.codingSystem, n.familyName, n.fullName, n.fundingProgram, n.geprisId, n.givenName, n.gndId, n.icd10code, n.identifierInPrimarySource, n.isniId, n.loincId, n.meshId, n.orcidId, n.rorId, n.sizeOfDataBasis, n.temporal, n.title, n.valueSet, n.version, n.viafId, n.wikidataId]
OPTIONS {indexConfig: $config};""",
        {
            "config": {
                "fulltext.eventually_consistent": True,
                "fulltext.analyzer": "german",
            }
        },
    )


def test_mocked_graph_seed_data(mocked_graph: MagicMock) -> None:
    graph = GraphConnector.get()
    graph._seed_data()

    assert mocked_graph.call_args_list[-6].args == (
        r"""MERGE (merged:MergedPrimarySource {identifier: $stable_target_id})
MERGE (extracted:ExtractedPrimarySource {identifier: $identifier})-[stableTargetId:stableTargetId {position: 0}]->(merged)
ON CREATE SET extracted = $on_create
ON MATCH SET extracted += $on_match
WITH extracted, [] as edges, [] as values
CALL {
    WITH values
    MATCH (:ExtractedPrimarySource {identifier: $identifier})-[]->(gc:Text|Link)
    WHERE NOT gc IN values
    DETACH DELETE gc
    RETURN count(gc) as pruned
}
RETURN extracted, edges, values, pruned;""",
        {
            "identifier": Identifier("00000000000000"),
            "stable_target_id": PrimarySourceID("00000000000000"),
            "on_match": {"version": None},
            "on_create": {
                "version": None,
                "identifier": "00000000000000",
                "identifierInPrimarySource": "mex",
            },
            "values": [],
        },
    )
    assert mocked_graph.call_args_list[-5].args == (
        r"""MATCH (fromNode:ExtractedAccessPlatform|ExtractedActivity|ExtractedContactPoint|ExtractedDistribution|ExtractedOrganization|ExtractedOrganizationalUnit|ExtractedPerson|ExtractedPrimarySource|ExtractedResource|ExtractedVariable|ExtractedVariableGroup {identifier:$source_node})
MATCH (toNode:MergedAccessPlatform|MergedActivity|MergedContactPoint|MergedDistribution|MergedOrganization|MergedOrganizationalUnit|MergedPerson|MergedPrimarySource|MergedResource|MergedVariable|MergedVariableGroup {identifier:$target_node})
MERGE (fromNode)-[edge:hadPrimarySource {position:$position}]->(toNode)
RETURN edge;""",
        {
            "position": 0,
            "source_node": "00000000000000",
            "target_node": "00000000000000",
        },
    )
    assert mocked_graph.call_args_list[-4].args == (
        r"""MATCH (fromNode:ExtractedAccessPlatform|ExtractedActivity|ExtractedContactPoint|ExtractedDistribution|ExtractedOrganization|ExtractedOrganizationalUnit|ExtractedPerson|ExtractedPrimarySource|ExtractedResource|ExtractedVariable|ExtractedVariableGroup {identifier:$source_node})
MATCH (toNode:MergedAccessPlatform|MergedActivity|MergedContactPoint|MergedDistribution|MergedOrganization|MergedOrganizationalUnit|MergedPerson|MergedPrimarySource|MergedResource|MergedVariable|MergedVariableGroup {identifier:$target_node})
MERGE (fromNode)-[edge:stableTargetId {position:$position}]->(toNode)
RETURN edge;""",
        {
            "position": 0,
            "source_node": "00000000000000",
            "target_node": "00000000000000",
        },
    )
    assert mocked_graph.call_args_list[-3].args == (
        r"""MERGE (merged:MergedPrimarySource {identifier: $stable_target_id})
MERGE (extracted:ExtractedPrimarySource {identifier: $identifier})-[stableTargetId:stableTargetId {position: 0}]->(merged)
ON CREATE SET extracted = $on_create
ON MATCH SET extracted += $on_match
WITH extracted, [] as edges, [] as values
CALL {
    WITH values
    MATCH (:ExtractedPrimarySource {identifier: $identifier})-[]->(gc:Text|Link)
    WHERE NOT gc IN values
    DETACH DELETE gc
    RETURN count(gc) as pruned
}
RETURN extracted, edges, values, pruned;""",
        {
            "identifier": Identifier("00000000000000"),
            "stable_target_id": PrimarySourceID("00000000000000"),
            "on_match": {"version": None},
            "on_create": {
                "version": None,
                "identifier": "00000000000000",
                "identifierInPrimarySource": "mex",
            },
            "values": [],
        },
    )
    assert mocked_graph.call_args_list[-2].args == (
        r"""MATCH (fromNode:ExtractedAccessPlatform|ExtractedActivity|ExtractedContactPoint|ExtractedDistribution|ExtractedOrganization|ExtractedOrganizationalUnit|ExtractedPerson|ExtractedPrimarySource|ExtractedResource|ExtractedVariable|ExtractedVariableGroup {identifier:$source_node})
MATCH (toNode:MergedAccessPlatform|MergedActivity|MergedContactPoint|MergedDistribution|MergedOrganization|MergedOrganizationalUnit|MergedPerson|MergedPrimarySource|MergedResource|MergedVariable|MergedVariableGroup {identifier:$target_node})
MERGE (fromNode)-[edge:hadPrimarySource {position:$position}]->(toNode)
RETURN edge;""",
        {
            "position": 0,
            "source_node": "00000000000000",
            "target_node": "00000000000000",
        },
    )
    assert mocked_graph.call_args_list[-1].args == (
        r"""MATCH (fromNode:ExtractedAccessPlatform|ExtractedActivity|ExtractedContactPoint|ExtractedDistribution|ExtractedOrganization|ExtractedOrganizationalUnit|ExtractedPerson|ExtractedPrimarySource|ExtractedResource|ExtractedVariable|ExtractedVariableGroup {identifier:$source_node})
MATCH (toNode:MergedAccessPlatform|MergedActivity|MergedContactPoint|MergedDistribution|MergedOrganization|MergedOrganizationalUnit|MergedPerson|MergedPrimarySource|MergedResource|MergedVariable|MergedVariableGroup {identifier:$target_node})
MERGE (fromNode)-[edge:stableTargetId {position:$position}]->(toNode)
RETURN edge;""",
        {
            "position": 0,
            "source_node": "00000000000000",
            "target_node": "00000000000000",
        },
    )


def test_mocked_graph_fetch_identities(mocked_graph: MagicMock) -> None:
    graph = GraphConnector.get()
    graph.fetch_identities(stable_target_id=Identifier.generate(99))

    assert mocked_graph.call_args_list[-1].args == (
        q.stable_target_id_identity(),
        {
            "had_primary_source": None,
            "identifier_in_primary_source": None,
            "stable_target_id": Identifier.generate(99),
            "limit": 1000,
        },
    )

    graph.fetch_identities(
        had_primary_source=Identifier.generate(101), identifier_in_primary_source="one"
    )

    assert mocked_graph.call_args_list[-1].args == (
        q.had_primary_source_and_identifier_in_primary_source_identity(),
        {
            "had_primary_source": Identifier.generate(101),
            "identifier_in_primary_source": "one",
            "stable_target_id": None,
            "limit": 1000,
        },
    )

    with pytest.raises(MExError, match="invalid identity query parameters"):
        graph.fetch_identities(identifier_in_primary_source="two")


def test_mocked_graph_merges_node(
    mocked_graph: MagicMock, dummy_data: list[AnyExtractedModel]
) -> None:
    extracted_organizational_unit = dummy_data[4]
    graph = GraphConnector.get()
    graph.merge_node(extracted_organizational_unit)

    assert (
        mocked_graph.call_args_list[-1].args[0]
        == r"""MERGE (merged:MergedOrganizationalUnit {identifier: $stable_target_id})
MERGE (extracted:ExtractedOrganizationalUnit {identifier: $identifier})-[stableTargetId:stableTargetId {position: 0}]->(merged)
ON CREATE SET extracted = $on_create
ON MATCH SET extracted += $on_match
MERGE (extracted)-[e0:name {position: 0}]->(v0:Text)
ON CREATE SET v0 = $values[0]
ON MATCH SET v0 += $values[0]
WITH extracted, [e0] as edges, [v0] as values
CALL {
    WITH values
    MATCH (:ExtractedOrganizationalUnit {identifier: $identifier})-[]->(gc:Text|Link)
    WHERE NOT gc IN values
    DETACH DELETE gc
    RETURN count(gc) as pruned
}
RETURN extracted, edges, values, pruned;"""
    )

    assert mocked_graph.call_args_list[-1].args[1] == {
        "identifier": "bFQoRhcVH5DHUz",
        "on_create": {
            "email": [],
            "identifier": "bFQoRhcVH5DHUz",
            "identifierInPrimarySource": "ou-1",
        },
        "on_match": {"email": []},
        "stable_target_id": "bFQoRhcVH5DHUy",
        "values": [{"language": "en", "value": "Unit 1"}],
    }


def test_mocked_graph_merges_edges(
    mocked_graph: MagicMock, dummy_data: list[AnyExtractedModel]
) -> None:
    extracted_activity = dummy_data[4]
    graph = GraphConnector.get()
    graph.merge_edges(extracted_activity)

    assert (
        mocked_graph.call_args_list[-1].args[0]
        == r"""MATCH (fromNode:ExtractedAccessPlatform|ExtractedActivity|ExtractedContactPoint|ExtractedDistribution|ExtractedOrganization|ExtractedOrganizationalUnit|ExtractedPerson|ExtractedPrimarySource|ExtractedResource|ExtractedVariable|ExtractedVariableGroup {identifier:$source_node})
MATCH (toNode:MergedAccessPlatform|MergedActivity|MergedContactPoint|MergedDistribution|MergedOrganization|MergedOrganizationalUnit|MergedPerson|MergedPrimarySource|MergedResource|MergedVariable|MergedVariableGroup {identifier:$target_node})
MERGE (fromNode)-[edge:stableTargetId {position:$position}]->(toNode)
RETURN edge;"""
    )
    assert mocked_graph.call_args_list[-1].args[1] == {
        "position": 0,
        "source_node": "bFQoRhcVH5DHUz",
        "target_node": "bFQoRhcVH5DHUy",
    }


@pytest.mark.usefixtures("mocked_graph")
def test_mocked_graph_ingests_models(dummy_data: list[AnyExtractedModel]) -> None:
    graph = GraphConnector.get()
    identifiers = graph.ingest(dummy_data)

    assert identifiers == [d.identifier for d in dummy_data]


@pytest.mark.usefixtures("load_dummy_data")
@pytest.mark.integration
def test_query_nodes() -> None:
    connector = GraphConnector.get()

    result = connector.query_nodes(None, None, None, 0, 1)

    assert result.all() == [
        {
            "items": [
                {
                    "entityType": "ExtractedPrimarySource",
                    "hadPrimarySource": ["00000000000000"],
                    "identifier": "00000000000000",
                    "identifierInPrimarySource": "mex",
                    "stableTargetId": ["00000000000000"],
                }
            ],
            "total": 7,
        }
    ]
