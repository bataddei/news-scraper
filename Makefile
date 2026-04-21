.PHONY: help install install-dev test lint format migrate e2e clean

help:
	@echo "Common tasks:"
	@echo "  make install       - install runtime dependencies"
	@echo "  make install-dev   - install runtime + dev dependencies (tests, linters)"
	@echo "  make test          - run unit tests"
	@echo "  make lint          - check code style"
	@echo "  make format        - auto-fix code style"
	@echo "  make migrate       - apply SQL migrations to Supabase (reads SUPABASE_DB_URL from .env)"
	@echo "  make e2e           - run the end-to-end timestamp test against Supabase"

install:
	python -m pip install -e .

install-dev:
	python -m pip install -e ".[dev]"

test:
	pytest -v

lint:
	ruff check src tests

format:
	ruff check --fix src tests
	ruff format src tests

migrate:
	python -m news_archive.scripts.run_migrations

e2e:
	python -m news_archive.scripts.end_to_end_test

run-fomc:
	python -m news_archive.collectors.run fed_fomc_statements

clean:
	rm -rf build dist *.egg-info .pytest_cache .ruff_cache .mypy_cache
	find . -type d -name __pycache__ -exec rm -rf {} +
