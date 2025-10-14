<# Validate whether an item with the given identifier and entity type exists.

Jinja:
    node_labels: Allowed labels of the to-be-checked item

Args:
    identifier: Identifier of the item to check

Returns:
    Boolean representing the existence of the requested item.
-#>
RETURN EXISTS {
    MATCH (:<<node_labels|join("|")>> {identifier: $identifier})
} AS exists;
