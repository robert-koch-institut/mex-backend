<% from "macros/check_reference_filters.cql" import check_reference_filters with context -%>

MATCH (extracted_or_rule_node:<<any_extracted_or_rule_label>>)-[:stableTargetId]->(merged_node:<<any_merged_label>>)
<<check_reference_filters()>>
RETURN extracted_or_rule_node, merged_node;
