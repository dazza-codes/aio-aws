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

coverage:
	@poetry run pytest -q \
		-W ignore::DeprecationWarning \
		-n auto \
		--cov-config .coveragerc \
		--verbose \
		--cov-report term \
		--cov-report html \
		--cov-report xml \
		--cov=$(LIB) tests

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
	@source "$(HOME)/.poetry/env"
	@rm -f poetry.lock
	@poetry run pip install --upgrade pip
	@poetry install -v --no-interaction

lint: clean
	@poetry run pylint --disable=missing-docstring tests
	@poetry run pylint $(LIB)

test: clean
	@poetry run pytest -n auto -q --durations=10 --show-capture=no --junitxml=report.xml tests

typehint: clean
	@poetry run mypy --follow-imports=skip $(LIB)

package: clean
	@poetry check
	@poetry build

package-check: package
	@poetry run twine check dist/*

publish: package-check
	# derivative projects can enable this
	# poetry run twine upload dist/$(LIB)-*.whl

poetry:
	@if ! which poetry > /dev/null; then \
		curl -sSL https://raw.githubusercontent.com/sdispater/poetry/master/get-poetry.py | python
	fi

.PHONY: clean coverage docs flake8 format init lint test typehint package package-check publish poetry
