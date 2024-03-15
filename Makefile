.PHONY: all test setup hooks install linter pytest wheel container run start docs
all: install test
test: linter pytest

LATEST = $(shell git describe --tags $(shell git rev-list --tags --max-count=1))

setup:
	# install meta requirements system-wide
	@ echo installing requirements; \
	python -m pip --quiet --disable-pip-version-check install --force-reinstall -r requirements.txt; \

hooks:
	# install pre-commit hooks when not in CI
	@ if [ -z "$$CI" ]; then \
		pre-commit install; \
	fi; \

install: setup hooks
	# install packages from lock file in local virtual environment
	@ echo installing package; \
	poetry install --no-interaction --sync; \

linter:
	# run the linter hooks from pre-commit on all files
	@ echo linting all files; \
	pre-commit run --all-files; \

pytest:
	# run the pytest test suite with unit tests only
	@ echo running unit tests; \
	poetry run pytest; \

wheel:
	# build the python package
	@ echo building wheel; \
	poetry build --no-interaction --format wheel; \

container:
	# build the docker image
	@ echo building docker image mex-backend:${LATEST}; \
	export DOCKER_BUILDKIT=1; \
	docker build \
		--tag rki/mex-backend:${LATEST} \
		--tag rki/mex-backend:latest .; \

run: container
	# run the service as a docker container
	@ echo running docker container mex-backend:${LATEST}; \
	docker run \
		--env MEX_BACKEND_HOST=0.0.0.0 \
		--publish 8080:8080 \
		rki/mex-backend:${LATEST}; \

start: container
	# start the service using docker-compose
	@ echo running docker-compose with mex-backend:${LATEST}; \
	export DOCKER_BUILDKIT=1; \
	export COMPOSE_DOCKER_CLI_BUILD=1; \
	docker-compose up; \

docs:
	# use sphinx to auto-generate html docs from code
	@ echo generating api docs; \
	poetry run sphinx-apidoc -f -o docs/source mex; \
	poetry run sphinx-build -aE -b dirhtml docs docs/dist; \
