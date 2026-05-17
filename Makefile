.PHONY: up down lint format test ingest pipeline

up:
	docker compose -f infra/docker-compose.yml up -d

down:
	docker compose -f infra/docker-compose.yml down -v

lint:
	ruff check . && black --check .

format:
	black . && ruff check --fix .

test:
	pytest tests/ -v

test-unit:
	pytest tests/unit/ -v

test-integration:
	pytest tests/integration/ -v

ingest:
	python -m ingestion.batch_ingest

pipeline:
	python -m processing.jobs.cleansing
	python -m processing.jobs.join_tables
	python -m processing.jobs.windowed_agg
	python -m modeling.dim_table
	python -m modeling.fact_table
	python -m quality.checks.curated_checks
