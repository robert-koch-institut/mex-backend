<# Fetch the full text search index by its static name.

Returns:
    node_labels: List of labels of nodes with searchable fields
    search_fields: List of names of searchable fields
-#>
SHOW INDEXES
YIELD name, labelsOrTypes, properties
WHERE name = "search_index"
RETURN labelsOrTypes AS node_labels, properties AS search_fields;
