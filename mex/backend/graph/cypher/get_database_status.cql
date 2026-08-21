<# Get the version of the neo4j kernel running the database.

Committing this query doubles as an availability check, because it only succeeds
when the configured database is reachable and online.

Returns:
    version: The neo4j kernel version as a string
-#>
CALL dbms.components()
YIELD name, versions
WHERE name = "Neo4j Kernel"
RETURN versions[0] AS version;
