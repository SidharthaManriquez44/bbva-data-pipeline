.PHONY: up down test integration lint format precommit

up:
	docker compose up -d

down:
	docker compose down -v

test:
	pytest

integration:
	pytest -m integration

lint:
	ruff check .

format:
	ruff format .

check: lint test

precommit:
	pre-commit run --all-files
