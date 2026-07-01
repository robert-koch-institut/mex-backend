<# Filter merged items by the outgoing references of all their components.

Collect the references of every component (extracted or rule node) of the merged
item, fill in a sentinel for missing fields, then assert all reference filters
are satisfied across the combined references. This ensures filters match even
when the referenced values live on different components, e.g. one reference on
the extracted item and another added by an editor via a rule.

Globals:
    any_extracted_or_rule_label: List of all extracted and rule class labels
    any_rule_label: List of all rule class labels
    any_merged_label: List of all merged class labels
    no_reference: Sentinel value for missing references
    mex_editor_primary_source: Stable target id of the mex-editor primary source

Jinja:
    reference_fields: List of relationship names to check

Args:
    merged_node: The merged node whose components' references to check

Returns:
    ref_matches: Aggregated reference data with structure:
        - field: Reference relationship type (from reference_fields)
        - identifier: Referenced node identifier or "<<no_reference>>" sentinel
    Query result: Filtered rows containing:
        - merged_node: The merged node entity
        - ref_matches: Reference matches list (as above)
-#>
<% macro filter_nodes_by_reference_filters() -%>
MATCH (component:<<any_extracted_or_rule_label>>)-[:stableTargetId]->(merged_node)
OPTIONAL MATCH (component)-[reference:<<reference_fields|join("|")>>]->(referenced_merged_node:<<any_merged_label>>)
WITH
    merged_node,
    collect(CASE WHEN reference IS NOT NULL THEN type(reference) END) AS found_fields,
    collect(CASE WHEN reference IS NOT NULL
        THEN {field: type(reference), identifier: referenced_merged_node.identifier} END
    ) AS existing_refs
WITH
    merged_node,
    existing_refs
    + [f IN $reference_fields WHERE NOT f IN found_fields | {field: f, identifier: "<<no_reference>>"}]
    <# TODO(ND): remove this crutch as soon as we add hadPrimarySource to our rules. -#>
    + CASE
        WHEN "hadPrimarySource" IN $reference_fields
         AND EXISTS {
            MATCH (component:<<any_rule_label>>)-[:stableTargetId]->(merged_node)
        }
        THEN [{field: "hadPrimarySource", identifier: "<<mex_editor_primary_source>>"}]
        ELSE []
      END
    AS ref_matches
WHERE ALL(rf IN $reference_filters WHERE
    ANY(m IN ref_matches WHERE m.field = rf.field AND m.identifier IN rf.identifiers)
)
<%- endmacro %>
