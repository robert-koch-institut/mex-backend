import pytest
from click.testing import CliRunner

from mex.artificial.helpers import create_artificial_items_and_rule_sets
from mex.backend.seed.main import main
from tests.conftest import get_graph


@pytest.mark.integration
def test_seed_entrypoint_ingests_artificial_data() -> None:
    """Run the seed entrypoint and checks items are ingested into DB."""
    count = 25
    seed = 1
    chattiness = 4
    locale = "de_DE"

    expected_items = [
        item
        for container in create_artificial_items_and_rule_sets(
            locale=locale,
            seed=seed,
            count=count,
            chattiness=chattiness,
        )
        for item in [container.extracted_item, container.rule_set]
        if item
    ]

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "--count",
            str(count),
            "--seed",
            str(seed),
            "--chattiness",
            str(chattiness),
            "--locale",
            locale,
        ],
    )

    assert result.exit_code == 0

    graph = get_graph()

    graph_identifiers = {node["identifier"] for node in graph if node.get("identifier")}

    expected_identifiers = {
        str(item.identifier) for item in expected_items if hasattr(item, "identifier")
    }

    assert len(graph_identifiers) > 0
   # graph_identifiers contains all IDs of expected_identifiers + IDs of merged items
    assert expected_identifiers <= graph_identifiers 
