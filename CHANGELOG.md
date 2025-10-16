# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- update mex-common dependency to 1.5
- persons have rki org as affiliation

### Changes

- temporarily lock fastapi to 0.118 because 0.119 breaks our swagger

### Deprecated

### Removed

### Fixed

- fixed allowed values for the `referenceField` filter parameter

### Security

## [1.1.1] - 2025-10-06

### Changes

- temporarily switch to redis to bitnamilegacy

## [1.1.0] - 2025-09-11

### Added

- ingest endpoint now supports rule-sets
- new endpoint returns merged person from ldap login information

### Changes

- ldap search endpoint now also returns contact points
- change rule set ingestion to use v2-style queries
- update mex-common to 1.4

### Removed

- removed black dependency in favor of unittest.mock

## [1.0.0] - 2025-08-25

### Changes

- improve parameter documentation for cypher queries
- change preview-merging logic to favor blocked values over empty fields

## [0.41.3] - 2025-07-24

### Changes

- update mex-common to 0.64

## [0.41.2] - 2025-07-24

### Added

- enable mypy type checking for tests

### Changes

- make clear that the v2 ingest is only compatible with extracted items, not rules, yet
- clean up v1 ingest and make it only compatible with rule-sets, until its deletion
- use a single transaction for v1 ingestion of a rule-set
- reorganize a few functions in the rules and graph modules for clarity and consistency
- update mex-common to 0.63

## [0.41.1] - 2025-07-08

### Fixed

- fix ldap connection resetting

## [0.41.0] - 2025-07-07

### Added

- add tests for local and redis cache connectors
- add endpoints to get a single merged or single extracted item by id
- add generic id filter to extracted, merge and preview endpoints

### Changes

- rename `GraphConnector.exists_merged_item` to `exists_item`
- improve a batch of doc-strings with args, raises and return sections

### Deprecated

- deprecate hadPrimarySource for extracted, merge and preview endpoints

### Fixed

- fix: ldap endpoint returns 500 if first search is more than an hour ago

## [0.40.0] - 2025-06-24

### Added

- BREAKING: add uniqueness constraint on provenance fields for extracted items

### Changes

- explicitly specify the location (Body, Path, Query) for all endpoint parameters
- make redis_url a secret string

## [0.39.1] - 2025-06-19

### Changes

- update neo4j docker tag to v2025.05

## [0.39.0] - 2025-06-17

### Changes

- use mex-common and mex-artificial from pypi

## [0.38.0] - 2025-05-22

### Changes

- BREAKING: merge item search skips invalid items

## [0.37.4] - 2025-05-20

### Added

- add settings for `graph_tx_timeout` and `graph_session_timeout`

### Changes

- explicitly configure read/write access level per query type
- narrow-down labels of link and create nodes to speed up ingest query
- improve `validate_ingested_data` with clearer error details

## [0.37.3] - 2025-05-19

### Added

- better logging for detailed and uncaught exception handling

## [0.37.2] - 2025-05-19

### Changes

- BREAKING: return status code 429 for retryable ingestion errors

## [0.37.1] - 2025-05-15

### Added

- log neo4j notification summary

### Changes

- use neo4j driver retry instead of backoff

## [0.37.0] - 2025-05-14

### Changes

- abort ingestion of further items after client disconnects
- wrap neo4j errors during ingestion in ingestion errors
- bumped cookiecutter template ed5deb

## [0.36.0] - 2025-05-12

### Changes

- cache assigned identities in redis to enable multiprocessing
- update mex-model and mex-common

## [0.35.0] - 2025-05-06

### Changes

- try out new ingestion logic with combined merging of nodes and edges

## [0.34.0] - 2025-04-29

### Added

- added `hadPrimarySource` parameter to extracted items search
- added `/v0/_system/metrics` endpoint for prometheus metrics
- add artificial data creator helper and use in test

### Changes

- change unit and primary source helpers from caching to graph lookup

### Removed

- BREAKING: remove response body from ingest endpoint

## [0.33.2] - 2025-04-16

### Removed

- remove startup tasks to seed primary sources and units

## [0.33.1] - 2025-04-15

### Changes

- optimize full text search query performance

## [0.33.0] - 2025-04-14

### Added

- seed primary sources and organigram on fastAPI startup

### Changes

- simplify and harmonize aux endpoints

## [0.32.0] - 2025-04-03

### Added

- ingest extracted organizational units on ldap search
- implement ORCID search endpoint returning persons

### Changes

- bumped common dependency to 0.56.0
- run ingest transactions in one single neo4j session to improve performance
- changed query to stop deduplication of merged edges
- reduced max time out of neo4j commits to 10 secs

## [0.31.5] - 2025-03-20

### Added

- ldap search ingests PrimarySource into backend

## [0.31.4] - 2025-03-18

### Changes

- further improve exceptions for edge merging failures with start and end node props

## [0.31.3] - 2025-03-12

### Changes

- bump cookiecutter template to 716a58

## [0.31.2] - 2025-03-11

### Fixed

- use the latest git tag for containerize job

## [0.31.1] - 2025-03-11

### Added

- show package version in system check endpoint

### Changes

- improve exception messages for edge merging failures

## [0.31.0] - 2025-03-05

### Added

- wikidata search ingests PrimarySource into backend

### Changes

- simplify `expand_references_in_search_result` (not operating inline anymore)
- update mex-common to 0.54.1

## [0.30.2] - 2025-02-27

### Changes

- preview and merged endpoint filter by multiple primary sources (was: one)

## [0.30.1] - 2025-02-17

### Added

- add option to filter by primary source to merged and preview endpoint

### Changes

- update mex-common to 0.51.1

## [0.30.0] - 2025-02-10

### Changes

- bumped cookiecutter template to robert-koch-institut/mex-template@5446da
- update mex-common to 0.50.0
- GraphConnector.ingest now accepts rule-set requests as well
- BREAKING: GraphConnector.ingest returns a list of ingested models, instead of ids
- POST /ingest now accepts rule-set requests as well as extracted items
- BREAKING: POST /ingest returns a container of ingested models, instead of ids

### Removed

- remove backend settings that were just duplicating common settings
- removed BackendIdentityProvider enum, because it is now included in common
- remove GraphConnector.create_rule_set, in favor of combined ingest method
- remove unused ingest_extracted_items_into_graph helper
- remove unused BulkIngestRequest and BulkIngestResponse

## [0.29.1] - 2025-01-24

### Changes

- use `create_merged_item` function from mex-common

## [0.29.0] - 2025-01-22

### Changes

- add support for full text queries on nested models to find extracted/rule/merged items
- optimize extracted/rule/merged search queries by applying sorting and pagination
  before pulling in nested models as well as identifiers from referenced merged items
  and by replacing subqueries with cypher "pattern comprehension" syntax
- prefix `components` in merged queries with `_`, to be more harmonious with `_refs`
- add email fields to `SEARCHABLE_FIELDS` and `SEARCHABLE_CLASSES` (stop-gap MX-1766)

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
