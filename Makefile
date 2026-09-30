SHELL := /bin/sh
PYTHON ?= python3
UV := .tools/uv/bin/uv
export UV_CACHE_DIR := $(CURDIR)/.cache/uv
export UV_PYTHON_INSTALL_DIR := $(CURDIR)/.tools/python
export TMPDIR := $(CURDIR)/.cache/tmp

.PHONY: setup test lint format check clean

setup:
	mkdir -p .cache/tmp .tools
	test -x "$(UV)" || ($(PYTHON) -m venv .tools/uv && .tools/uv/bin/python -m pip install --no-cache-dir 'uv==0.12.21')
	"$(UV)" sync --locked

test:
	.venv/bin/python -m pytest

lint:
	.venv/bin/ruff check .
	.venv/bin/ruff format --check .

format:
	.venv/bin/ruff check --fix .
	.venv/bin/ruff format .

check: lint test

clean:
	rm -rf .pytest_cache .ruff_cache build dist htmlcov .coverage
