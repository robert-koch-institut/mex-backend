<# Check preconditions for matching an extracted item to a new merged item.

This query validates that the conditions for matching an extracted item to
a different merged item are met before the actual matching operation is performed.
Passed preconditions return `true`, failed preconditions `false` and unverifiable
preconditions returns `null`.

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
    not_self_match: Optional boolean indicating this is not a noop self-match
    same_merged_type: Optional boolean indicating the items have the same stem types
    merged_type_allowed: Optional boolean indicating the extracted type is not blocked
-#>
OPTIONAL MATCH (extracted:<<extracted_labels|join("|")>> {identifier: $extracted_identifier})
OPTIONAL MATCH (extracted)-[:stableTargetId]->(old_merged:<<merged_labels|join("|")>>)
OPTIONAL MATCH (old_merged)<-[:stableTargetId]-(old_rules:<<rule_labels|join("|")>>)
OPTIONAL MATCH (new_merged:<<merged_labels|join("|")>> {identifier: $merged_identifier})
WITH
   count(DISTINCT extracted) = 1 AS extracted_exists,
   count(old_rules) = 3 AS old_rules_exist,
   count(DISTINCT new_merged) = 1 AS merged_exists,
   elementId(old_merged) <> elementId(new_merged) AS not_self_match,
   labels(old_merged) = labels(new_merged) AS same_merged_type,
   NOT ANY(label IN labels(new_merged) WHERE label IN $blocked_types) AS merged_type_allowed
RETURN *;
