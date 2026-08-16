BINARIES := cdc-applier snapshot-loader reconciler repair-worker controlplane
GO       ?= go
GOFLAGS  ?=
LDFLAGS  := -s -w -X main.version=$(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

.PHONY: build
build: ## Build all binaries into ./bin
	@mkdir -p bin
	@for b in $(BINARIES); do \
		echo "  building $$b"; \
		$(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o bin/$$b ./cmd/$$b || exit 1; \
	done

.PHONY: test
test: ## Run unit tests (no external dependencies required)
	$(GO) test -race -count=1 ./...

.PHONY: test-short
test-short: ## Run unit tests without the race detector
	$(GO) test -count=1 ./...

.PHONY: cover
cover: ## Run tests and report coverage per package
	$(GO) test -count=1 -coverprofile=coverage.out ./...
	@$(GO) tool cover -func=coverage.out | tail -30

.PHONY: integration
integration: ## Run integration tests against the local stack (requires: make up)
	$(GO) test -count=1 -tags=integration ./test/integration/...

.PHONY: vet
vet: ## Run go vet
	$(GO) vet ./...

.PHONY: lint
lint: ## Run golangci-lint
	@command -v golangci-lint >/dev/null 2>&1 || { echo "golangci-lint not installed: https://golangci-lint.run"; exit 1; }
	golangci-lint run ./...

.PHONY: fmt
fmt: ## Format all Go source
	gofmt -w -s .

.PHONY: check
check: fmt vet test ## Format, vet and test

.PHONY: up
up: ## Start the local stack (Postgres, MySQL, Kafka)
	docker compose up -d
	@echo "waiting for services to become healthy..."
	@docker compose ps

.PHONY: down
down: ## Stop the local stack and remove volumes
	docker compose down -v

.PHONY: logs
logs: ## Tail the local stack logs
	docker compose logs -f

.PHONY: migrate-pg
migrate-pg: ## Apply the control schema to the local Postgres
	docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U migration -d target \
		< migrations/postgres/0001_control_schema.sql

.PHONY: migrate-mysql
migrate-mysql: ## Apply the control schema to the local MySQL
	docker compose exec -T mysql mysql -u root -prootpw < migrations/mysql/0001_control_schema.sql

.PHONY: genkey
genkey: ## Generate development key material
	@echo "export MIGRATION_STATIC_KEY=$$(openssl rand -base64 32)"

.PHONY: docker
docker: ## Build the service container image
	docker build -f deploy/docker/Dockerfile -t db-migration-platform:local .

.PHONY: clean
clean: ## Remove build artefacts
	rm -rf bin coverage.out
