from collections import deque

import click

from mex.artificial.helpers import create_artificial_items_and_rule_sets
from mex.backend.graph.connector import GraphConnector
from mex.backend.settings import BackendSettings


@click.command()
@click.option("--count", default=25, type=int)
@click.option("--seed", default=1, type=int)
@click.option("--chattiness", default=8, type=int)
@click.option("--locale", default="de_DE", type=str)
def main(
    count: int,
    seed: int,
    chattiness: int,
    locale: str,
) -> None:
    """Seed the backend database with artificial test data.

    Args:
        count: Number of items to generate.
        seed: Random seed.
        chattiness: Verbosity level.
        locale: Language/ Region code ("de_DE").
    """
    BackendSettings.get()
    data = [
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

    connector = GraphConnector.get()
    deque(connector.ingest_items(data))
