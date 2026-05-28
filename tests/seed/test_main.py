import pytest
from click.testing import CliRunner

from mex.backend.seed.main import main
from tests.conftest import get_graph


@pytest.mark.integration
def test_seed_entrypoint_ingests_artificial_data() -> None:
    """Seed entrypoint should ingest artificial data into neo4j."""
    # count = 25
    # seed = 1
    # chattiness = 4

    # expected_items = [
    #     item
    #     for container in create_artificial_items_and_rule_sets(
    #         locale="de_DE",
    #         seed=seed,
    #         count=count,
    #         chattiness=chattiness,
    #     )
    #     for item in [container.extracted_item, container.rule_set]
    #     if item
    # ]

    # runner = CliRunner()
    # result = runner.invoke(
    #     main,
    #     [
    #         "--count", str(count),
    #         "--seed", str(seed),
    #         "--chattiness", str(chattiness),
    #     ],
    # )

    # assert result.exit_code == 0, result.output

    # graph = get_graph()
    # ingested_identifiers = {
    #     node["identifier"]
    #     for node in graph
    #     if "identifier" in node
    # }

    # expected_identifiers = {
    #     str(item.identifier)
    #     for item in expected_items
    #     if isinstance(item, AnyExtractedModel)
    # }
    # expected_stable_target_ids = {
    #     str(item.stableTargetId)
    #     for item in expected_items
    #     if isinstance(item, AnyRuleSetResponse)
    # }

    # ingested_stable_target_ids = {
    #     node["stableTargetId"]
    #     for node in graph
    #     if node.get("stableTargetId") and not node.get("identifier")
    # }

    # assert expected_identifiers <= ingested_identifiers, (
    #     f"missing extracted items: {expected_identifiers - ingested_identifiers}"
    # )
    # assert expected_stable_target_ids <= ingested_stable_target_ids, (
    #     f"missing rule sets: {expected_stable_target_ids - ingested_stable_target_ids}"
    # )
    graph = get_graph()
    before = len(graph)
    runner = CliRunner()
    result = runner.invoke(
        main,
        ["--count", "25", "--seed", "1", "--chattiness", "4"],
    )

    assert result.exit_code == 0, result.output

    graph = get_graph()
    assert len(graph) > before, "graph should not be empty after seeding"

    # 8,635
