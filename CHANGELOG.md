# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- add `ldap.py` for an ldap endpoint for getting a person by string
- added parameterized tests for it in `test_ldap.py`

### Changes

### Deprecated

### Removed

### Fixed

### Security

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
