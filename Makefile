.RECIPEPREFIX = >
.DEFAULT_GOAL := help

COMPOSE ?= docker compose
PYTHON ?= python

.PHONY: help
help: ## Show this help
> @grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-16s %s\n", $$1, $$2}'

.PHONY: up
up: ## Build and start postgres, rabbitmq, api and consumer
> $(COMPOSE) up -d --build

.PHONY: down
down: ## Stop the stack and remove the containers
> $(COMPOSE) down

.PHONY: clean
clean: ## Stop the stack and delete the volumes
> $(COMPOSE) down -v

.PHONY: logs
logs: ## Follow the api and consumer logs
> $(COMPOSE) logs -f api consumer

.PHONY: ps
ps: ## Show the container status
> $(COMPOSE) ps

.PHONY: migrate
migrate: ## Apply the database migrations
> $(COMPOSE) run --rm migrate

.PHONY: revision
revision: ## Create a migration: make revision m="add refunds table"
> $(COMPOSE) run --rm migrate alembic revision --autogenerate -m "$(m)"

.PHONY: shell
shell: ## Open a shell inside the api container
> $(COMPOSE) exec api /bin/bash

.PHONY: psql
psql: ## Open psql on the postgres container
> $(COMPOSE) exec postgres psql -U $${POSTGRES_USER:-payments} -d $${POSTGRES_DB:-payments}

.PHONY: rabbitmq
rabbitmq: ## Open the RabbitMQ management UI (http://localhost:15672)
> @echo "http://localhost:$${RABBITMQ_MANAGEMENT_PORT:-15672}"

.PHONY: demo
demo: ## Start the stack together with the demo webhook receiver
> $(COMPOSE) --profile demo up -d --build

.PHONY: test
test: ## Run the test suite
> $(PYTHON) -m pytest

.PHONY: lint
lint: ## Run ruff and mypy
> $(PYTHON) -m ruff check .
> $(PYTHON) -m mypy app

.PHONY: format
format: ## Format the code
> $(PYTHON) -m ruff format .
> $(PYTHON) -m ruff check --fix .