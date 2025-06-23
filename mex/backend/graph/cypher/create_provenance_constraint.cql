<# Create a uniqueness constraint on the provenance fields for one node label at a time.

Args:
    node_label: Label of node for which to create a constraint, e.g. ExtractedActivity
-#>
CREATE CONSTRAINT <<node_label|dromedary_to_snake>>_provenance_uniqueness IF NOT EXISTS
FOR (n:<<node_label>>)
REQUIRE (n.hadPrimarySource, n.identifierInPrimarySource) IS UNIQUE;
