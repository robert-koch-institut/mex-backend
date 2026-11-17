<% from "macros/normalize_vocabulary_values.cql" import normalize_vocabulary_values with context -%>

MATCH (node:<<any_extracted_label>>)
WHERE node.identifier = $identifier
UNWIND $vocabulary_fields AS field
RETURN field, <<normalize_vocabulary_values("node", "field")>> AS values;
