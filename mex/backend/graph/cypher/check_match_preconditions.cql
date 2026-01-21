<# Check preconditions for matching an extracted item to a new merged item.

This query validates that all necessary conditions are met before allowing
an extracted item to be matched to a different merged item.

Globals:
    extracted_labels: List of all extracted class labels
    merged_labels: List of all merged class labels

Args:
    extracted_identifier: Identifier of the extracted item to be matched
    merged_identifier: Identifier of the new target merged item
    blocked_types: List of merged entity types that cannot be matched

Returns:
    extracted_exists: Boolean indicating if the extracted item exists
    old_rules_exist: Boolean indicating if the old merged item has complete rules
    merged_exists: Boolean indicating if the target merged item exists
    not_self_match: Boolean indicating this is not a noop self-match
    same_merged_type: Boolean indicating the items have the same stem types
    merged_type_allowed: Boolean indicating the extracted type is not blocked
-#>
OPTIONAL MATCH (extracted:<<extracted_labels|join("|")>> {identifier: $extracted_identifier})-[:stableTargetId]->(old_merged:<<merged_labels|join("|")>>)
OPTIONAL MATCH (old_merged)<-[:stableTargetId]-(old_rules:<<merged_labels|join("|")>>)
OPTIONAL MATCH (new_merged:<<merged_labels|join("|")>> {identifier: $merged_identifier})
OPTIONAL MATCH (new_merged)<-[:stableTargetId]-(new_rules:<<merged_labels|join("|")>>)
WITH
   count(extracted) = 1 AS extracted_exists,
   count(old_rules) = 3 AS old_rules_exist,
   count(new_merged) = 1 AS merged_exists,
   elementId(old_merged) <> elementId(new_merged) AS not_self_match,
   labels(old_merged) = labels(new_merged) AS same_merged_type,
   NOT ANY(label IN labels(new_merged) WHERE label IN $blocked_types) AS merged_type_allowed
RETURN *;
