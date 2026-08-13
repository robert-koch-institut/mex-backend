<# Move all extracted items of one merged item over to another merged item.

Re-links the `stableTargetId` edges of the goner's extracted items to the keeper.
The ingest query deliberately never garbage-collects `stableTargetId` edges, so this
cannot be expressed by simply re-ingesting the extracted items with a new stable
target id: they would end up attached to both merged items.

Rule items are not moved. The goner keeps its own rule set, because that is where its
`supersededBy` reference lives once it has been merged away.

Globals:
    any_extracted_label: pipe delimited sequence of all extracted class labels
    any_merged_label: pipe delimited sequence of all merged class labels

Args:
    goner_identifier: Identifier of the merged item that is merged away
    keeper_identifier: Identifier of the merged item that survives the merge

Returns:
    moved_extracted_count: Number of extracted items moved to the keeper
    moved_identities: Provenance of every moved extracted item, so that stale entries
        can be dropped from the identity cache once the merge is committed
-#>
MATCH (keeper:<<any_merged_label>> {identifier: $keeper_identifier})
MATCH (extracted:<<any_extracted_label>>)-[old:stableTargetId]->(:<<any_merged_label>> {identifier: $goner_identifier})

MERGE (extracted)-[:stableTargetId {position: 0}]->(keeper)
DELETE old

RETURN
    count(extracted) AS moved_extracted_count,
    collect({
        hadPrimarySource: [(extracted)-[:hadPrimarySource]->(primary_source) | primary_source.identifier][0],
        identifierInPrimarySource: extracted.identifierInPrimarySource
    }) AS moved_identities;
