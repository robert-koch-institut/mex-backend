<# Check preconditions for merging a goner merged item into a keeper merged item.

This query validates that the conditions for merging one merged item into another
merged item are met before the actual merging operation is performed.
Passed preconditions return `true`, failed preconditions `false` and unverifiable
preconditions return `null`.

Globals:
    any_merged_label: pipe delimited sequence of all merged class labels
    any_rule_label: pipe delimited sequence of all rule class labels

Args:
    goner_identifier: Identifier of the merged item that is merged away
    keeper_identifier: Identifier of the merged item that survives the merge
    non_mergeable_types: List of merged entity types that cannot be merged

Returns:
    goner_exists: Boolean indicating whether the goner merged item exists
    keeper_exists: Boolean indicating whether the keeper merged item exists
    not_self_merge: Boolean indicating whether this is not a noop self-merge
    same_merged_type: Boolean indicating whether the items have the same stem types
    merging_of_type_is_allowed: Boolean indicating whether types are allowed to merge
    goner_not_superseded: Boolean indicating whether the goner was not merged away yet
    keeper_not_superseded: Boolean indicating whether the keeper is not a tombstone
-#>
OPTIONAL MATCH (goner:<<any_merged_label>> {identifier: $goner_identifier})
OPTIONAL MATCH (keeper:<<any_merged_label>> {identifier: $keeper_identifier})
WITH
   goner,
   keeper,
   count(DISTINCT goner) = 1 AS goner_exists,
   count(DISTINCT keeper) = 1 AS keeper_exists,
   elementId(goner) <> elementId(keeper) AS not_self_merge,
   labels(goner) = labels(keeper) AS same_merged_type,
   NOT ANY(label IN labels(keeper) WHERE label IN $non_mergeable_types) AS merging_of_type_is_allowed
RETURN
   goner_exists,
   keeper_exists,
   not_self_merge,
   same_merged_type,
   merging_of_type_is_allowed,
   CASE WHEN goner IS NULL THEN null ELSE
      NOT EXISTS { (goner)<-[:stableTargetId]-(:<<any_rule_label>>)-[:supersededBy]->() }
   END AS goner_not_superseded,
   CASE WHEN keeper IS NULL THEN null ELSE
      NOT EXISTS { (keeper)<-[:stableTargetId]-(:<<any_rule_label>>)-[:supersededBy]->() }
   END AS keeper_not_superseded;
