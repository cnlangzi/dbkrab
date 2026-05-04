# dbkrab Makefile
#
# Usage:
#   make build     - Build the binary
#   make test      - Run tests
#   make run       - Run with example config
#   make clean     - Clean build artifacts
#   make lint      - Run linters
#   make coverage  - Generate test coverage report
#   make install   - Install binary to /usr/local/bin

BINARY_NAME := dbkrab
BUILD_DIR := bin
CMD_DIR := cmd/app
GO := go
# -p 1 limits parallelism to single process (useful for limited memory environments)
TEST_PARALLEL ?= -p 1

# Version info
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
BUILD_TIME := $(shell date -u '+%Y-%m-%d_%H:%M:%S')
LDFLAGS := -ldflags "-X main.Version=$(VERSION) -X main.BuildTime=$(BUILD_TIME)"

.PHONY: all build test clean lint coverage install help

all: build

## build: Build the binary
build:
	@echo "Building $(BINARY_NAME)..."
	@mkdir -p $(BUILD_DIR)
	$(GO) build $(GOFLAGS) $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME) ./$(CMD_DIR)
	@echo "Built: $(BUILD_DIR)/$(BINARY_NAME)"

## test: Run all tests
test:
	@echo "Running tests..."
	$(GO) test -v -race $(TEST_PARALLEL) ./... 2>&1 | \
		grep -vE '(^=== RUN\s|^\s+--- PASS|^\s+--- FAIL\t|^--- PASS\t|^--- PASS:|^\?\s|INFO\s|DEBUG\s)' || true

## test-short: Run short tests
test-short:
	@echo "Running short tests..."
	$(GO) test -v -short ./...

## coverage: Generate test coverage report
coverage:
	@echo "Generating coverage report..."
	$(GO) test -v -race -coverprofile=coverage.out ./...
	$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"


.PHONY: start-dev stop-dev reset-dev
## dev: Run from project directory (code/dbkrab)
start-dev: 
	@echo "Running dev..."
	cd ./cmd/app && go build -o dbkrab . && ./dbkrab

stop-dev:
	@echo "Stopping dev..."
	pkill -f "./dbkrab" 2>/dev/null || true

reset-dev: stop-dev
	@echo "Resetting dev state..."
	@rm -rf ./cmd/app/data/app/*
	@rm -rf ./cmd/app/data/sinks/*/*.db ./cmd/app/data/sinks/*/*.db-shm ./cmd/app/data/sinks/*/*.db-wal
	@rm -rf ./cmd/app/logs/*
	@mkdir -p ./cmd/app/logs

## clean: Clean build artifacts
clean:
	@echo "Cleaning..."
	@rm -rf $(BUILD_DIR)
	@rm -f coverage.out coverage.html
	@rm -rf data/*.db data/*.json
	$(GO) clean

## lint: Run golangci-lint
lint:
	@echo "Running linters..."
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run --jobs=$(or $(GOLANGCI_JOBS),1) ./...; \
	else \
		echo "golangci-lint not installed. Run: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest"; \
		$(GO) vet ./...; \
	fi

## vet: Run go vet
vet:
	@echo "Running go vet..."
	$(GO) vet -p $(or $(GOLANGCI_JOBS),1) ./...

## fmt: Format code
fmt:
	@echo "Formatting code..."
	$(GO) fmt ./...

## mod: Download and tidy dependencies
mod:
	@echo "Tidy modules..."
	$(GO) mod tidy
	$(GO) mod download

## deploy: Deploy to /opt/dbkrab (build, install binary and config)
deploy:
	@echo "Deploying to /opt/dbkrab..."
	@./scripts/deploy.sh config.yml
	@echo "Binary deployed"

## install: Deploy and setup systemd service
install: deploy
	@echo "Setting up systemd service..."
	@sudo cp scripts/dbkrab.service /etc/systemd/system/
	@sudo systemctl daemon-reload
	@sudo systemctl enable dbkrab
	@sudo systemctl restart dbkrab
	@echo "✅ Installed and started"

## status: Show service status
status:
	@if [ -f /var/run/dbkrab.pid ]; then \
		PID=$$(cat /var/run/dbkrab.pid); \
		if kill -0 $$PID 2>/dev/null; then \
			echo "✅ dbkrab running (PID: $$PID)"; \
			curl -s http://localhost:9021/api/health; \
		else \
			echo "❌ dbkrab not running (stale PID file)"; \
		fi; \
	else \
		echo "❌ dbkrab not running (no PID file)"; \
	fi

## logs: Show recent logs
logs:
	@tail -50 /var/log/dbkrab/dbkrab.log

## stop: Stop dbkrab (via systemd)
stop:
	@echo "Stopping dbkrab..."
	@sudo systemctl stop dbkrab
	@sleep 1
	@echo "✅ Stopped"

## start: Start dbkrab (via systemd)
start:
	@echo "Starting dbkrab..."
	@sudo systemctl start dbkrab
	@sleep 3
	@if systemctl is-active --quiet dbkrab; then \
		echo "✅ Started"; \
	else \
		echo "❌ Failed to start"; \
	fi

## restart: Restart dbkrab (via systemd)
restart: stop start

## reset: Reset dbkrab state (clear DB and logs under /opt/dbkrab) and restart via systemd
reset:
	@echo "Resetting dbkrab state..."
	@make stop || true
	@sleep 1
	@echo "Deleting app data files..."
	@rm -rf /opt/dbkrab/data/app/*
	@echo "Deleting sinks data files..."
	@rm -rf /opt/dbkrab/data/sinks/*/*.db /opt/dbkrab/data/sinks/*/*.db-shm /opt/dbkrab/data/sinks/*/*.db-wal
	@echo "Deleting logs..."
	@rm -f /opt/dbkrab/logs/*.log
	@touch /opt/dbkrab/logs/dbkrab.log
	@chmod 666 /opt/dbkrab/logs/dbkrab.log
	@make start
	@if systemctl is-active --quiet dbkrab; then \
		echo "✅ dbkrab reset complete"; \
	else \
		echo "❌ dbkrab failed to start"; \
	fi

## uninstall: Remove binary from /usr/local/bin
uninstall:
	@echo "Uninstalling..."
	sudo rm -f /usr/local/bin/$(BINARY_NAME)
	@echo "Uninstalled."

## docker-build: Build Docker image
docker-build:
	@echo "Building Docker image..."
	docker build -t dbkrab:$(VERSION) .

## docker-run: Run Docker container
docker-run:
	@echo "Running Docker container..."
	docker run -it --rm dbkrab:$(VERSION)

## hooks-install: Install git hooks
hooks-install:
	@echo "Installing git hooks..."
	@mkdir -p .git/hooks
	@cp scripts/git-hooks/pre-commit .git/hooks/pre-commit
	@chmod +x .git/hooks/pre-commit
	@echo "✅ Git hooks installed"

## hooks-uninstall: Remove git hooks
hooks-uninstall:
	@echo "Removing git hooks..."
	@rm -f .git/hooks/pre-commit
	@echo "✅ Git hooks removed"

## pre-commit: Run pre-commit checks (for manual testing)
pre-commit: vet test-short
	@echo "✅ Pre-commit checks passed"

## version: Show version
version:
	@echo "Version: $(VERSION)"
	@echo "Build Time: $(BUILD_TIME)"

## help: Show this help
help:
	@echo "dbkrab - Lightweight MSSQL CDC in Go"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@sed -n 's/^## //p' $(MAKEFILE_LIST) | column -t -s ':'

.DEFAULT_GOAL := help