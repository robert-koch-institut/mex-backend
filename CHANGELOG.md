# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

### Changes

- add support for full text queries on nested models to find extracted/rule/merged items
- optimize extracted/rule/merged search queries by applying sorting and pagination
  before pulling in nested models as well as identifiers from referenced merged items
  and by replacing subqueries with cypher "pattern comprehension" syntax
- prefix `components` in merged queries with `_`, to be more harmonious with `_refs`
- add email fields to `SEARCHABLE_FIELDS` and `SEARCHABLE_CLASSES` (stop-gap MX-1766)

### Deprecated

### Removed

### Fixed

### Security

## [0.28.0] - 2025-01-15

### Added

- add `extracted_or_rule_labels` to query builder globals
- add two matched organizations to the test dummy data

### Changes

- rename short and obscure cypher query variables to more expressive and verbose ones
- rename `stable_target_id` to more appropriate `identifier` argument for merged queries

### Fixed

- avoid recursive retries in `GraphConnector._check_connectivity_and_authentication`
- fix integration tests not properly marked as integration tests

## [0.27.0] - 2024-12-19

### Added

- configure backoff rules for graph commits
- validate that the number of merged edges is as intended
- implement graph flushing connector method
- add endpoint for flushing the neo4j database (when running in debug)

### Removed

- remove open-api schema customization, not needed anymore by the current editor
- remove purge-script, this is an HTTP endpoint now

## [0.26.0] - 2024-12-18

### Added

- added an endpoint for getting a person by name from LDAP

### Changes

- updated to mex-common 0.45.0 and mex-model 3.4.0

## [0.25.0] - 2024-12-10

### Added

- allow item merging functions to ignore cardinality and output preview items

### Changes

- harmonize error handling for transforming raw rule-sets to responses
- return 404 on GET rule-set endpoint, when no rules are found
- create new endpoint for fetching previews of merged items
- replaced `mex.backend.fields` with `mex.common` counterpart

### Removed

- removed not needed `mex.backend.constants` module
- removed over-engineered `reraising` function

## [0.24.0] - 2024-11-25

### Changes

- clean up non-functional cypher query style issues

### Fixed

- do not raise server error when search query is not found

## [0.23.0] - 2024-11-19

### Changes

- improve error handling by returning validation issues for InconsistentGraphErrors
- pin mex-release to 0.3.0

## [0.22.0] - 2024-11-08

### Changes

- upgrade mex-common and mex-model dependencies to metadata model v3
- apply additional linters in prep for `all` ruff linters
- mute warnings about labels used in queries but missing in graph
- split up search_merged_items_in_graph for better readability
- update cypher queries to use `CALL` clauses with correct variable scope
- BREAKING: drop support for neo4j server version 5.6 and lower

## [0.21.0] - 2024-10-31

### Changes

- upgrade mex-common and mex-model dependencies to metadata model v3
- apply additional linters in prep for `all` ruff linters

## [0.20.0] - 2024-10-18

### Changes

- silence neo4j missing label warnings, because we will likely never need all labels
- sort search results by `identifier` and `entityType` to ensure a more stable order
- improve handling of pydantic validation errors and uncaught errors

### Removed

- remove already obsolete module `mex.backend.serialization`
  this is not needed any more with the new mex-common version

### Fixed

- fix how merged edges are counted (currently only used for debugging)

## [0.19.1] - 2024-09-18

### Fixed

- pin jinja as explicit dependency

## [0.19.0] - 2024-09-18

### Added

- add `GraphConnector.fetch_rule_items` to get rule items
- add `GET /rule-set/{stableTargetId}` endpoint to get rule-sets
- add a rule-set response to the `create_rule_set` endpoint
- implement merging logic as a triple of functions corresponding to our rule types
- add a preview endpoint to perform merge with a submitted rule-set and all found items
- add GraphConnector.exists_merged_item to verify stableTargetIds exist
- add PUT endpoint to update rule-sets for existing merged items

### Changes

- BREAKING: move `to_primitive` to a more fittingly named `mex.backend.serialization`
- BREAKING: swap `INDEXABLE_MODEL_CLASSES_BY_NAME` for `ALL_MODEL_CLASSES_BY_NAME`
  to also include non-indexable models (namely: merged models)
- BREAKING: rename `fetch_extracted_data` to a more consistent `fetch_extracted_items`
- harmonize PagedAuxiliaryResponse with Merged- and ExtractedItemSearchResponse
- move searching and fetching of extracted and merged items to `helpers` module
  so they can be reused more easily outside of the endpoint code
- use starlette status code constants instead of plain integers for readability
- BREAKING: rework `create_rule` connector method and endpoint to use RuleSets
- merged endpoint returns actual merged items. was: extracted items presented as merged
  items
- improved test coverage for graph connector and wikidata endpoint

### Removed

- remove redundant `status_code=200` config on endpoints
- drop unused `UnprefixedType` support for `entityType` parameters

### Fixed

- fix empty graph search results not returning empty lists but raising errors
- move `purge-db` from entrypoints to pdm scripts, because it's not part of the module

## [0.18.1] - 2024-08-07

### Fixed

- make merged-items facade endpoint more lenient towards validation errors

## [0.18.0] - 2024-08-05

### Changes

- BulkIngestRequest contains now one single list "items"
- tests for ingestion adapted to BulkIngestRequest-Model
- remove stop-gaps for MX-1596

### Removed

- removed class _BaseBulkIngestRequest for ingestion model

## [0.17.0] - 2024-07-29

### Changes

- BREAKING: use `items` instead of `results` in paged wiki response
- downgrade query logging to log level `debug`

### Fixed

- fix endpoint serializing not working with `mex.common.types`

## [0.16.0] - 2024-07-26

### Added

- `/wikidata` endpoint to fetch all matching organizations from wikidata
- add support for computed fields in graph queries

### Changes

- BREAKING: make `MEX_EXTRACTED_PRIMARY_SOURCE` an instance of its own class
  instead of ExtractedPrimarySource in order to set static provenance identifiers

## [0.15.0] - 2024-06-14

### Added

- add `INDEXABLE_MODEL_CLASSES_BY_NAME` lookup to fields module
- add `render_constraints` jinja filter
- implement new `POST /rule-item` endpoint
- db purge script

### Changes

- update connector and queries to support creating rules (analogous to extracted items)

## [0.14.0] - 2024-06-03

### Changes

- re-implemented queries as templated cql files
- updated graph connector for new queries
- improved isolation of neo4j dependency
- improved documentation and code-readability
- move exception handling middleware to new module
- change `identity_provider` default to `MEMORY`
- add stop-gap code waiting to be resolved by mx-1596
- migrate to latest `mex-common`

### Removed

- trashed hydration module

## [0.12.0] - 2024.04.02

### Added

- pull request template
- sphinx documentation
- publish cve results
- CHANGELOG file
- cruft template link
- open-code workflow
- graph identity provider that assigns ids to extracted data
- generalize type enums into DynamicStrEnum superclass
- seed primary source for mex on connector init
- test fixture that makes Identifiers deterministic

### Changes

- harmonized boilerplate
- assign name to uniqueness constraint
- use graph identity provider in identity endpoints
- add module name to dynamic models for better debugging
- allow 'MergedThing' as well as 'Thing' as entityType query parameter

### Removed

- remove dynamic extracted model classes and use those from mex-common

### Fixed

- don't allow identifierInPrimarySource changes on node updates
