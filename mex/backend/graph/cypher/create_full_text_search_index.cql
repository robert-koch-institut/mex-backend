CREATE FULLTEXT INDEX search_index IF NOT EXISTS
FOR (n:<<node_labels|join("|")>>)
ON EACH [<<search_fields|map("ensure_prefix", "n.")|join(", ")>>]
OPTIONS {indexConfig: $index_config};
