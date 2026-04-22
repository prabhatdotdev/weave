.PHONY: help test test-verbose test-coverage test-coverage-check test-recovery bench lint fmt vet build rabbitmq-start rabbitmq-stop kafka-start kafka-stop clean install-deps

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-20s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install-deps: ## Install dependencies
	@echo "Installing dependencies..."
	@go mod download
	@go mod tidy

test: ## Run tests
	@echo "Running tests..."
	@go test -v ./...

test-verbose: ## Run tests with verbose output
	@go test -v -race -cover ./...

test-coverage: ## Run coverage for critical packages and generate report
	@echo "Running coverage for critical packages..."
	@./scripts/check_coverage.sh

test-coverage-check: ## Enforce minimum coverage for critical packages
	@echo "Checking critical package coverage threshold..."
	@./scripts/check_coverage.sh
test-recovery: ## Run recovery-focused tests for connection loss and reconnect scenarios
	@echo "Running recovery tests..."
	@go test -v -run "Recovery|recovery|Reconnect|reconnect" ./transport/amqp ./transport/kafka
	@echo "✅ Recovery tests passed"

bench: ## Run benchmarks
	@echo "Running benchmarks..."
	@go test -bench=. -benchmem ./...

lint: ## Run linter (requires golangci-lint)
	@echo "Running linter..."
	@golangci-lint run

fmt: ## Format code
	@echo "Formatting code..."
	@go fmt ./...

vet: ## Run go vet
	@echo "Running go vet..."
	@go vet ./...

build: ## Build the library
	@echo "Building..."
	@go build ./...

rabbitmq-start: ## Start RabbitMQ with Docker Compose
	@echo "Starting RabbitMQ..."
	@docker-compose up -d rabbitmq
	@echo "Waiting for RabbitMQ to be ready..."
	@sleep 5
	@echo "RabbitMQ is ready!"
	@echo "Management UI: http://localhost:15672 (guest/guest)"

rabbitmq-stop: ## Stop RabbitMQ
	@echo "Stopping RabbitMQ..."
	@docker-compose stop rabbitmq

kafka-start: ## Start Kafka with Docker Compose
	@echo "Starting Kafka..."
	@docker-compose up -d kafka zookeeper
	@echo "Waiting for Kafka to be ready..."
	@sleep 10
	@echo "Kafka is ready at localhost:9092"

kafka-stop: ## Stop Kafka
	@echo "Stopping Kafka..."
	@docker-compose stop kafka zookeeper

rabbitmq-logs: ## Show RabbitMQ logs
	@docker-compose logs -f rabbitmq

kafka-logs: ## Show Kafka logs
	@docker-compose logs -f kafka

clean: ## Clean build artifacts and test files
	@echo "Cleaning..."
	@rm -f coverage.out coverage.html coverage.critical.out coverage.critical.html
	@go clean
