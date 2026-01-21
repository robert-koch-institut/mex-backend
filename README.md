# MEx backend

Backend server for the RKI metadata exchange.

[![cookiecutter](https://github.com/robert-koch-institut/mex-backend/actions/workflows/cookiecutter.yml/badge.svg)](https://github.com/robert-koch-institut/mex-template)
[![cve-scan](https://github.com/robert-koch-institut/mex-backend/actions/workflows/cve-scan.yml/badge.svg)](https://github.com/robert-koch-institut/mex-backend/actions/workflows/cve-scan.yml)
[![documentation](https://github.com/robert-koch-institut/mex-backend/actions/workflows/documentation.yml/badge.svg)](https://robert-koch-institut.github.io/mex-backend)
[![linting](https://github.com/robert-koch-institut/mex-backend/actions/workflows/linting.yml/badge.svg)](https://github.com/robert-koch-institut/mex-backend/actions/workflows/linting.yml)
[![open-code](https://github.com/robert-koch-institut/mex-backend/actions/workflows/open-code.yml/badge.svg)](https://gitlab.opencode.de/robert-koch-institut/mex/mex-backend)
[![testing](https://github.com/robert-koch-institut/mex-backend/actions/workflows/testing.yml/badge.svg)](https://github.com/robert-koch-institut/mex-backend/actions/workflows/testing.yml)

## Project

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
[project page](https://www.rki.de/DE/Aktuelles/Publikationen/Forschungsdaten/MEx/metadata-exchange-plattform-mex-node.html).

[^1]: FAIR is referencing the so-called
[FAIR data principles](https://www.go-fair.org/fair-principles/) – guidelines to make
data Findable, Accessible, Interoperable and Reusable.

**Contact** \
For more information, please feel free to email us at [mex@rki.de](mailto:mex@rki.de).

### Publisher

**Robert Koch-Institut** \
Nordufer 20 \
13353 Berlin \
Germany

## Package

The `mex-backend` package is a multi-purpose backend application with an HTTP-API. It
provides endpoints to ingest data from ETL-pipelines, for a metadata editor application,
and for publishing pipelines to extract standardized data for use in upstream frontend
applications.

## License

This package is licensed under the [MIT license](/LICENSE). All other software
components of the MEx project are open-sourced under the same license as well.

## Development

### Installation

- on unix, consider using pyenv https://github.com/pyenv/pyenv
  - get pyenv `curl https://pyenv.run | bash`
  - install 3.11 `pyenv install 3.11`
  - switch version `pyenv global 3.11`
  - run `make install`
- on windows, consider using pyenv-win https://pyenv-win.github.io/pyenv-win/
  - follow https://pyenv-win.github.io/pyenv-win/#quick-start
  - install 3.11 `pyenv install 3.11`
  - switch version `pyenv global 3.11`
  - run `.\mex.bat install`

### Database

- for local development, neo4j desktop edition is recommended
  - make sure you download and run the same version as in `testing.yml`
  - also make sure db name, user and password match the `settings.py`
- for production deployments, a container runtime is recommended
  - for a configuration example, see `compose.yaml`

### Linting and testing

- run all linters with `.\mex.bat lint`
- run only unit tests with `.\mex.bat unit`
- run unit and integration tests with `.\mex.bat test`

### Updating dependencies

- update boilerplate files with `cruft update`
- update global requirements in `requirements.txt` manually
- update git hooks with `pre-commit autoupdate`
- update package dependencies using `uv sync --upgrade`
- update github actions in `.github/workflows/*.yml` manually

### Creating release

- run `mex release RULE` to release a new version where RULE determines which part of
  the version to update and is one of `major`, `minor`, `patch`.

### Container workflow

- build image with `make image`
- run directly using docker `make run`
- start with docker compose `make start`

## Commands

- run `uv run {command} --help` to print instructions
- run `uv run {command} --debug` for interactive debugging

### Backend

- `backend` starts the backend service
- `testing-backend` starts the testing backend service
