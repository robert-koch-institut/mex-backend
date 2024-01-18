# MEx backend

Backend server for the RKI metadata exchange.

[![testing](https://github.com/robert-koch-institut/mex-backend/actions/workflows/testing.yml/badge.svg)](https://github.com/robert-koch-institut/mex-backend/actions/workflows/testing.yml)
[![linting](https://github.com/robert-koch-institut/mex-backend/actions/workflows/linting.yml/badge.svg)](https://github.com/robert-koch-institut/mex-backend/actions/workflows/linting.yml)
[![cve-scan](https://github.com/robert-koch-institut/mex-backend/actions/workflows/cve-scan.yml/badge.svg)](https://github.com/robert-koch-institut/mex-backend/actions/workflows/cve-scan.yml)
[![documentation](https://github.com/robert-koch-institut/mex-backend/actions/workflows/documentation.yml/badge.svg)](https://robert-koch-institut.github.io/mex-backend)

## project

The Metadata Exchange (MEx) project is committed to improve the retrieval of RKI
research data and projects. How? By focusing on metadata: instead of providing the
actual research data directly, the MEx metadata catalog captures descriptive information
about research data and activities. On this basis, we want to make the data FAIR[^1] so
that it can be shared with others.

Via MEx, metadata will be made findable, accessible and shareable, as well as available
for further research. The goal is to get an overview of what research data is available,
understand its context, and know what needs to be considered for subsequent use.

RKI cooperated with D4L data4life gGmbH for a pilot phase where the vision of a
FAIR metadata catalog was explored and concepts and prototypes were developed.
The partnership has ended with the successful conclusion of the pilot phase.

After an internal launch, the metadata will also be made publicly available and thus be
available to external researchers as well as the interested (professional) public to
find research data from the RKI.

For further details, please consult our
[project page](https://www.rki.de/DE/Content/Forsch/MEx/MEx_node.html).

[^1]: FAIR is referencing the so-called
[FAIR data principles](https://www.go-fair.org/fair-principles/) – guidelines to make
data Findable, Accessible, Interoperable and Reusable.

## package

The `mex-backend` package is a multi-purpose backend application with an HTTP-API. It
provides endpoints to ingest data from ETL-pipelines, for a metadata editor application,
and for publishing pipelines to extract standardized data for use in upstream frontend
applications.

## license

This package is licensed under the [MIT license](/LICENSE). All other software
components of the MEx project are open-sourced under the same license as well.

## development

### installation

- on unix, consider using pyenv https://github.com/pyenv/pyenv
  - get pyenv `curl https://pyenv.run | bash`
  - install 3.11 `pyenv install 3.11`
  - create env `pyenv virtualenv 3.11 mex`
  - go to repo root
  - use env `pyenv local mex`
  - run `make install`
- on windows, see https://python-poetry.org/docs/managing-environments
  - install `python3.11` in your preferred way
  - go to repo root
  - run `.\mex.bat install`

### linting and testing

- on unix run `make test`
- on windows run `.\mex.bat test`
- or run manually
  - linter checks via `pre-commit run --all-files`
  - all tests via `poetry run pytest`
  - just unit tests via `poetry run pytest -m "not integration"`

### updating dependencies

- update boilerplate files with `cruft update`
- update global dependencies in `requirements.txt` manually
- update git hooks with `pre-commit autoupdate`
- show outdated dependencies with `poetry show --outdated`
- update dependencies in poetry using `poetry update --lock`
- update github actions manually in `.github/workflows/*.yml`

### creating release

- update version, eg `poetry version minor`
- commit update `git commit --message "..." pyproject.toml`
- create a tag `git tag ...`
- push `git push --follow-tags`

### container workflow

- build container with `make container`
- run directly using docker `make run`
- start with docker-compose `make start`

## commands

- run `poetry run {command} --help` to print instructions
- run `poetry run {command} --debug` for interactive debugging

### backend

- `backend` starts the backend service
