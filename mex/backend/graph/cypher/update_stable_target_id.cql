<# TODO add docstring
-#>
MATCH(n :Extracted$stemType) WHERE n.identifier = $extracted_item_identifier
MATCH(m :Merged$stemType) WHERE m.identifier = $merged_item_identifier
MERGE (n)-[:stableTargetId {position:0}]->(m);
