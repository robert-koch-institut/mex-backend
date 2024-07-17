<# Validate whether a merged item with the given identifier and type exists.

Args:
    merged_label: Label of the to-be-checked merged item

Returns:
    Boolean representing the existence of the requested item.
-#>
RETURN EXISTS {
    MATCH (:<<merged_label>> {identifier: $identifier})
} as exists;
