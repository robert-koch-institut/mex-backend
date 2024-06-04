import click
from neo4j import GraphDatabase

from mex.backend.settings import BackendSettings
from mex.common.cli import entrypoint


@entrypoint(BackendSettings)
def purge_db() -> None:
    """Purge graph db."""
    settings = BackendSettings.get()
    if click.confirm(f'Purging graph db "{settings.graph_db}", Continue?:'):
        click.echo("Purging...")
        with GraphDatabase.driver(
            settings.graph_url,
            auth=(
                settings.graph_user.get_secret_value(),
                settings.graph_password.get_secret_value(),
            ),
            database=settings.graph_db,
        ) as driver:
            driver.execute_query("MATCH (n) DETACH DELETE n;")
            for row in driver.execute_query("SHOW ALL CONSTRAINTS;").records:
                driver.execute_query(f"DROP CONSTRAINT {row['name']};")
            for row in driver.execute_query("SHOW ALL INDEXES;").records:
                driver.execute_query(f"DROP INDEX {row['name']};")
        click.echo("Done")
