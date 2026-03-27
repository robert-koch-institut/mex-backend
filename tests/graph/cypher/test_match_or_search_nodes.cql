<% from "macros/match_or_search_nodes.cql" import match_or_search_nodes with context -%>

<<match_or_search_nodes()>>
ORDER BY merged_node.identifier, extracted_or_rule_node.identifier, head(labels(extracted_or_rule_node)) ASC
RETURN extracted_or_rule_node, merged_node;
