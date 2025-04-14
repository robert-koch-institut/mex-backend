import pytest

from mex.backend.graph.connector import GraphConnector


@pytest.mark.parametrize("name", ["ldap", "orcid", "wikidata"])
@pytest.mark.usefixtures("client")
@pytest.mark.integration
def test_extracted_primary_source_startup_tasks(name: str) -> None:
    # verify the primary sources are seeded on startup and are stored in the database
    graph = GraphConnector.get()
    result = graph.fetch_extracted_items(
        name,
        None,
        ["ExtractedPrimarySource"],
        0,
        1,
    )
    assert result["total"] == 1
    assert result["items"][0]["identifierInPrimarySource"] == name
