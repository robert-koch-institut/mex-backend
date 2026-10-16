<# Acquire exclusive write locks on the two merged items taking part in a merge.

Neo4j transactions run in read-committed isolation and reads acquire no locks.
Without this query, two concurrent merges of the same goner would both pass the
`goner_not_superseded` precondition before either writes its tombstone, and both
would commit, leaving the goner superseded by two different keepers.
The tombstone write alone does not prevent this: it locks the touched nodes only
once the stale precondition result has already been read.
Writing to a node takes an exclusive lock on it that is held until the end of the
transaction, so this no-op write makes any later read in the same transaction see
the committed state of a competing merge instead of a stale one.
See: https://neo4j.com/docs/operations-manual/current/database-internals/concurrent-data-access/

Locks are taken in identifier order, so that two merges with swapped goner and
keeper cannot grab the same two nodes in opposite order and deadlock.
Missing merged items are skipped, to leave reporting them to the precondition check.

Globals:
    any_merged_label: pipe delimited sequence of all merged class labels

Args:
    goner_identifier: Identifier of the merged item that is merged away
    keeper_identifier: Identifier of the merged item that survives the merge
-#>
UNWIND [$goner_identifier, $keeper_identifier] AS lock_identifier
WITH DISTINCT lock_identifier ORDER BY lock_identifier
OPTIONAL MATCH (lock_node:<<any_merged_label>> {identifier: lock_identifier})
FOREACH (_ IN CASE WHEN lock_node IS NULL THEN [] ELSE [1] END |
   SET lock_node._lock = true
   REMOVE lock_node._lock
);
