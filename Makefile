# https://www.gnu.org/software/make/manual/html_node/Makefile-Conventions.html

SHELL = /bin/bash

.ONESHELL:
.SUFFIXES:

LIB = aio_aws

clean:
	@rm -rf build dist .eggs *.egg-info
	@rm -rf .benchmarks .coverage coverage.xml htmlcov prof report.xml .tox
	@find . -type d -name '.mypy_cache' -exec rm -rf {} +
	@find . -type d -name '__pycache__' -exec rm -rf {} +
	@find . -type d -name '*pytest_cache*' -exec rm -rf {} +
	@find . -type f -name "*.py[co]" -exec rm -rf {} +

docs: clean
	@cd docs
	@rm -rf _build
	@poetry run make html
	@poetry run doc8
	@echo -e "\033[95m\n\nBuild successful! View the docs homepage at docs/_build/html/index.html.\n\033[0m"

flake8: clean
	@poetry run flake8 --ignore=E501 $(LIB)

format: clean
	@poetry run black $(LIB) tests docs *.py

init: poetry
	@poetry run pip install --upgrade pip
	@poetry install -v --no-interaction --extras all

lint: clean
	@poetry run pylint --disable=missing-docstring tests
	@poetry run pylint $(LIB)

test: clean
	@poetry run pytest \
		--durations=10 \
		--show-capture=no \
		--cov-config .coveragerc \
		--cov-report html \
		--cov-report term \
		--cov=$(LIB) tests

typehint: clean
	@poetry run mypy --follow-imports=skip $(LIB)

package: clean
	@poetry check
	@poetry build

package-check: package
	@poetry run twine check dist/*

publish: package-check
	@poetry publish

poetry:
	@if ! command -v poetry > /dev/null; then \
		curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python - ; \
		source "$(HOME)/.poetry/env" ; \
	fi

poetry-export:
	poetry export --without-hashes -f requirements.txt -o requirements.txt
	sed -i -e 's/^-e //g' requirements.txt

.PHONY: clean docs flake8 format lint typehint
.PHONY: init test package package-check publish poetry poetry-export
