SHELL := /bin/sh
PYTHON ?= python3
UV := .tools/uv/bin/uv
DOCKER ?= docker
KAFKA_CONSUME_MAX_MESSAGES ?= 1
export UV_CACHE_DIR := $(CURDIR)/.cache/uv
export UV_PYTHON_INSTALL_DIR := $(CURDIR)/.tools/python
export TMPDIR := $(CURDIR)/.cache/tmp

.PHONY: setup test lint format check generator clean
.PHONY: kafka-up kafka-down kafka-topics kafka-status kafka-demo integration-test
.PHONY: kafka-consume-assets kafka-consume-events kafka-consume-snapshots
.PHONY: spark-setup spark-test spark-stream spark-integration-test
SPARK_ARGS ?=

setup:
	mkdir -p .cache/tmp .tools
	test -x "$(UV)" || ($(PYTHON) -m venv .tools/uv && .tools/uv/bin/python -m pip install --no-cache-dir 'uv==0.12.21')
	"$(UV)" sync --locked --extra kafka

test:
	.venv/bin/python -m pytest tests/unit

spark-setup: setup
	"$(UV)" sync --locked --extra kafka --extra spark

spark-test:
	.venv/bin/python -m pytest tests/spark

spark-stream:
	.venv/bin/python -m spark.streaming.main $(SPARK_ARGS)

spark-integration-test:
	.venv/bin/python -m pytest -m spark_integration tests/spark_integration

lint:
	.venv/bin/ruff check .
	.venv/bin/ruff format --check .

format:
	.venv/bin/ruff format .
	.venv/bin/ruff check --fix .

check: lint test

generator:
	@.venv/bin/python -m generator.main --assets 5 --events 10 --seed 42 --no-sleep

kafka-up:
	$(DOCKER) compose up -d --wait --wait-timeout 180

kafka-down:
	$(DOCKER) compose down

kafka-topics:
	.venv/bin/python -m messaging.topics

kafka-status:
	$(DOCKER) compose ps

kafka-demo:
	.venv/bin/python -m generator.main --assets 3 --events 8 --rate 100 --seed 42 --no-sleep --sink kafka

integration-test:
	.venv/bin/python -m pytest -m integration tests/integration

kafka-consume-assets kafka-consume-events kafka-consume-snapshots:
	$(DOCKER) compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
		--bootstrap-server kafka:29092 --topic config.$(patsubst kafka-consume-%,%,$@) \
		--from-beginning --timeout-ms 10000 --max-messages $(KAFKA_CONSUME_MAX_MESSAGES) \
		--property print.key=true

clean:
	rm -rf .pytest_cache .ruff_cache build dist htmlcov .coverage
