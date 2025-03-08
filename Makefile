GOLANGCI_VERSION = v1.64.6

help: ## show help, shown by default if no target is specified
	@grep -E '^[0-9a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

lint: ## run code linters
	golangci-lint run

test: ## run tests
	go test -race ./...

test-integration: ## run integration tests
	go test -tags integration -race -p 1 ./...

test-integration-coverage: ## run integration tests with coverage
	go test -tags integration -race -p 1 ./... -coverprofile .testCoverage -covermode=atomic -coverpkg=./...
	go tool cover -func .testCoverage | grep total | awk '{print "Total coverage: "$$3}'

test-coverage: ## run unit tests and create test coverage
	CGO_ENABLED=0 go test ./... -coverprofile .testCoverage -covermode=atomic -coverpkg=./...
	go tool cover -func .testCoverage | grep total | awk '{print "Total coverage: "$$3}'

test-coverage-web: test-coverage ## run unit tests and show test coverage in browser
	go tool cover -func .testCoverage | grep total | awk '{print "Total coverage: "$$3}'
	go tool cover -html=.testCoverage

install-linters: ## install all used linters
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@${GOLANGCI_VERSION}
