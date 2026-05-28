from mex.artificial.helpers import create_artificial_items_and_rule_sets
from mex.backend.graph.connector import GraphConnector
from mex.backend.settings import BackendSettings
from mex.common.cli import entrypoint


@entrypoint(BackendSettings)
def main() -> None:
    """Entrypoint for seed script."""
    settings = BackendSettings.get()
    settings.debug = True

    count = 25
    seed = 1
    chattiness = 5

    containers = create_artificial_items_and_rule_sets(
        locale="de_DE",
        seed=seed,
        count=count,
        chattiness=chattiness,
    )

    items = [
        item
        for container in containers
        for item in (container.extracted_item, container.rule_set)
        if item
    ]

    connector = GraphConnector.get()
    connector.ingest_items(items)
