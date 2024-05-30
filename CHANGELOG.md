# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

### Changes

- re-implemented queries as templated cql files
- updated graph connector for new queries
- improved isolation of neo4j dependency
- improved documentation and code-readability
- move exception handling middleware to new module
- change `identity_provider` default to `MEMORY`
- add stop-gap code waiting to be resolved by mx-1596
- migrate to latest `mex-common`

### Deprecated

### Removed

- trashed hydration module

### Fixed

### Security

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
