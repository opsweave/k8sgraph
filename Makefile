.PHONY: build test lint fmt vet clean tidy

# Build the library (verify it compiles)
build:
	go build ./...

# Run tests
test:
	go test -v ./...

# Run tests with coverage
test-coverage:
	go test -v -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

# Run linter
lint:
	@which golangci-lint > /dev/null || (echo "Installing golangci-lint..." && go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest)
	golangci-lint run

# Format code
fmt:
	go fmt ./...

# Run go vet
vet:
	go vet ./...

# Clean build artifacts
clean:
	rm -f coverage.out coverage.html

# Tidy dependencies
tidy:
	go mod tidy

# Run all checks
check: fmt vet lint test

# Update dependencies
update:
	go get -u ./...
	go mod tidy
