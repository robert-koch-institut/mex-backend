<# Count the references to a goner merged item that a merge failed to move away.

After a merge, only the goner's own rules may point at it: its extracted items moved
to the keeper and every other reference to it was redirected to the keeper.
This includes the `stableTargetId` edges of leftover extracted items.

Globals:
    any_merged_label: pipe delimited sequence of all merged class labels
    any_extracted_label: pipe delimited sequence of all extracted class labels
    any_rule_label: pipe delimited sequence of all rule class labels

Args:
    goner_identifier: Identifier of the merged item that was merged away

Returns:
    leftover_count: Number of edges from other items still pointing at the goner
-#>
MATCH (goner:<<any_merged_label>> {identifier: $goner_identifier})
RETURN COUNT {
   MATCH (source:<<any_extracted_label>>|<<any_rule_label>>)-[]->(goner)
   WHERE NOT (
      source:<<any_rule_label>> AND EXISTS { (source)-[:stableTargetId]->(goner) }
   )
} AS leftover_count;
