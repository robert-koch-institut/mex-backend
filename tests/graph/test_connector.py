from unittest.mock import MagicMock, call

import pytest

from mex.backend.graph import queries as q
from mex.backend.graph.connector import GraphConnector
from mex.common.exceptions import MExError
from mex.common.models import ExtractedPerson
from mex.common.types import Identifier


@pytest.mark.usefixtures("mocked_graph")
def test_mocked_graph_init() -> None:
    graph = GraphConnector.get()
    result = graph.commit("MATCH (this);")
    assert result.model_dump() == {"data": []}


def test_mocked_graph_seed_constraints(mocked_graph: MagicMock) -> None:
    graph = GraphConnector.get()
    graph._seed_constraints()

    assert mocked_graph.run.call_args_list[-1] == call(
        """
CREATE CONSTRAINT identifier_uniqueness IF NOT EXISTS
FOR (n:ExtractedVariableGroup)
REQUIRE n.identifier IS UNIQUE;
""",
        None,
    )


def test_mocked_graph_seed_indices(mocked_graph: MagicMock) -> None:
    graph = GraphConnector.get()
    graph._seed_indices()

    assert mocked_graph.run.call_args_list[-1] == call(
        """
CREATE FULLTEXT INDEX text_fields IF NOT EXISTS
FOR (n:ExtractedAccessPlatform|ExtractedActivity|ExtractedContactPoint|ExtractedDistribution|ExtractedOrganization|\
ExtractedOrganizationalUnit|ExtractedPerson|ExtractedPrimarySource|ExtractedResource|ExtractedVariable|ExtractedVariableGroup)
ON EACH [n.abstract_value, n.alternativeName_value, n.alternativeTitle_value, \
n.description_value, n.instrumentToolOrApparatus_value, n.keyword_value, \
n.label_value, n.methodDescription_value, n.method_value, n.name_value, \
n.officialName_value, n.qualityInformation_value, n.resourceTypeSpecific_value, \
n.rights_value, n.shortName_value, n.spatial_value, n.title_value]
OPTIONS {indexConfig: $config};
""",
        {
            "config": {
                "fulltext.eventually_consistent": True,
                "fulltext.analyzer": "german",
            }
        },
    )


def test_mocked_graph_fetch_identities(mocked_graph: MagicMock) -> None:
    graph = GraphConnector.get()

    graph.fetch_identities(stable_target_id=Identifier.generate(99))
    assert mocked_graph.run.call_args.args == (
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
    assert mocked_graph.run.call_args.args == (
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
    mocked_graph: MagicMock, extracted_person: ExtractedPerson
) -> None:
    graph = GraphConnector.get()
    graph.merge_node(extracted_person)

    assert (
        mocked_graph.run.call_args.args[0]
        == """
MERGE (n:ExtractedPerson {identifier:$identifier})
ON CREATE SET n = $on_create
ON MATCH SET n += $on_match
RETURN n;
"""
    )
    assert mocked_graph.run.call_args.args[1] == {
        "identifier": str(extracted_person.identifier),
        "on_create": {
            "email": ["fictitiousf@rki.de", "info@rki.de"],
            "familyName": ["Fictitious"],
            "fullName": ["Fictitious, Frieda, Dr."],
            "givenName": ["Frieda"],
            "identifier": "bFQoRhcVH5DHUw",
            "identifierInPrimarySource": "frieda",
            "isniId": [],
            "orcidId": [],
            "stableTargetId": "bFQoRhcVH5DHVu",
        },
        "on_match": {
            "email": ["fictitiousf@rki.de", "info@rki.de"],
            "familyName": ["Fictitious"],
            "fullName": ["Fictitious, Frieda, Dr."],
            "givenName": ["Frieda"],
            "isniId": [],
            "orcidId": [],
        },
    }


def test_mocked_graph_merges_edges(
    mocked_graph: MagicMock, extracted_person: ExtractedPerson
) -> None:
    graph = GraphConnector.get()
    graph.merge_edges(extracted_person)
    mocked_graph.assert_has_calls(
        [
            call.run(
                """
MATCH (s {identifier:$fromIdentifier})
MATCH (t {stableTargetId:$toStableTargetId})
MERGE (s)-[e:hadPrimarySource]->(t)
RETURN e;
""",
                {
                    "fromIdentifier": str(extracted_person.identifier),
                    "toStableTargetId": str(extracted_person.hadPrimarySource),
                },
            )
        ]
    )


def test_mocked_graph_ingests_models(
    mocked_graph: MagicMock, extracted_person: ExtractedPerson
) -> None:
    graph = GraphConnector.get()
    identifiers = graph.ingest([extracted_person])

    assert identifiers == [extracted_person.identifier]

    # expect node is created
    assert mocked_graph.run.call_args_list[-5:][0][0] == (
        """
MERGE (n:ExtractedPerson {identifier:$identifier})
ON CREATE SET n = $on_create
ON MATCH SET n += $on_match
RETURN n;
""",
        {
            "identifier": "bFQoRhcVH5DHUw",
            "on_create": {
                "stableTargetId": "bFQoRhcVH5DHVu",
                "email": ["fictitiousf@rki.de", "info@rki.de"],
                "familyName": ["Fictitious"],
                "fullName": ["Fictitious, Frieda, Dr."],
                "givenName": ["Frieda"],
                "isniId": [],
                "orcidId": [],
                "identifier": "bFQoRhcVH5DHUw",
                "identifierInPrimarySource": "frieda",
            },
            "on_match": {
                "email": ["fictitiousf@rki.de", "info@rki.de"],
                "familyName": ["Fictitious"],
                "fullName": ["Fictitious, Frieda, Dr."],
                "givenName": ["Frieda"],
                "isniId": [],
                "orcidId": [],
            },
        },
    )
    # expect edges are created
    assert mocked_graph.run.call_args_list[-5:][1][0] == (
        """
MATCH (s {identifier:$fromIdentifier})
MATCH (t {stableTargetId:$toStableTargetId})
MERGE (s)-[e:hadPrimarySource]->(t)
RETURN e;
""",
        {
            "fromIdentifier": str(extracted_person.identifier),
            "toStableTargetId": str(extracted_person.hadPrimarySource),
        },
    )
    assert mocked_graph.run.call_args_list[-5:][2][0] == (
        """
MATCH (s {identifier:$fromIdentifier})
MATCH (t {stableTargetId:$toStableTargetId})
MERGE (s)-[e:affiliation]->(t)
RETURN e;
""",
        {
            "fromIdentifier": str(extracted_person.identifier),
            "toStableTargetId": str(extracted_person.affiliation[0]),
        },
    )
    assert mocked_graph.run.call_args_list[-5:][3][0] == (
        """
MATCH (s {identifier:$fromIdentifier})
MATCH (t {stableTargetId:$toStableTargetId})
MERGE (s)-[e:affiliation]->(t)
RETURN e;
""",
        {
            "fromIdentifier": str(extracted_person.identifier),
            "toStableTargetId": str(extracted_person.affiliation[1]),
        },
    )
    assert mocked_graph.run.call_args_list[-5:][4][0] == (
        """
MATCH (s {identifier:$fromIdentifier})
MATCH (t {stableTargetId:$toStableTargetId})
MERGE (s)-[e:memberOf]->(t)
RETURN e;
""",
        {
            "fromIdentifier": str(extracted_person.identifier),
            "toStableTargetId": str(extracted_person.memberOf[0]),
        },
    )


@pytest.mark.usefixtures("load_dummy_data")
@pytest.mark.integration
def test_query_nodes() -> None:
    connector = GraphConnector.get()

    result = connector.query_nodes(None, None, None, 0, 1)

    assert result.data == []
