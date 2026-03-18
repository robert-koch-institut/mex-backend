<% from "macros/collect_references_and_nested.cql" import collect_references_and_nested with context -%>

MATCH (extracted_or_rule_node:<<any_extracted_or_rule_label>>)
WHERE extracted_or_rule_node.identifier = $identifier
RETURN <<collect_references_and_nested()>> AS refs;
