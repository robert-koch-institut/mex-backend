<# Fetch the rule set belonging to one merged item as a rule-set-response shaped map.

Globals:
    any_rule_label: pipe delimited sequence of all rule class labels
    any_merged_label: pipe delimited sequence of all merged class labels
    rule_fields: list of rule set field names, e.g. additive, subtractive, ...

Args:
    identifier: Identifier of the merged item whose rule set to fetch

Returns:
    ruleSetResponse: map with one key per rule field plus the stable target id
        <rule_field>: rule item, or null if absent
            entityType: Label of the node
            _refs: Values of nested objects as well as referenced identifiers
            *nodeProps: All other properties of the node
        stableTargetId: identifier of the merged item

    When the merged item has no rule nodes, zero rows are returned.
-#>
<% from "macros/collect_references_and_nested.cql" import collect_references_and_nested with context -%>

MATCH (extracted_or_rule_node:<<any_rule_label>>)-[:stableTargetId]->(merged_node:<<any_merged_label>>)
WHERE merged_node.identifier = $identifier
WITH
    merged_node,
    extracted_or_rule_node,
    <<collect_references_and_nested()|indent(4)>> AS refs
WITH
    merged_node,
    collect(
        extracted_or_rule_node{.*, entityType: head(labels(extracted_or_rule_node)), _refs: refs}
    ) AS rules
RETURN {
    <%- for field in rule_fields %>
    <<field>>: head([rule IN rules WHERE rule.entityType STARTS WITH "<<field|capitalize>>"]),
    <%- endfor %>
    stableTargetId: merged_node.identifier
} AS ruleSetResponse;
