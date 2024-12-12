<# Validate whether a merged item with the given identifier and type exists.

Args:
    node_labels: Allowed labels of the to-be-checked merged item

Returns:
    Boolean representing the existence of the requested item.
-#>
RETURN EXISTS {
    MATCH (:<<node_labels|join("|")>> {identifier: $identifier})
} AS exists;
