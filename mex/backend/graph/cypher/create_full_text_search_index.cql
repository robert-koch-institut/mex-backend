<# Create a full text search index for faster searches on the given fields.

Jinja:
    node_labels: List of labels of nodes that have searchable fields
    search_fields: List of names of searchable fields

Args:
    index_config: Configuration mapping for the search index
-#>
CREATE FULLTEXT INDEX search_index IF NOT EXISTS
FOR (n:<<node_labels|join("|")>>)
ON EACH [<<search_fields|map("ensure_prefix", "n.")|join(", ")>>]
OPTIONS {indexConfig: $index_config};
