.PHONY: build run clean test install

# Build the binary
build:
	go build -o metric-sender main.go

# Run without building
run:
	go run main.go

# Build and run
build-run: build
	./metric-sender

# Clean build artifacts
clean:
	rm -f metric-sender metric-sender-*

# Install dependencies (none for this project)
deps:
	go mod download

# Format code
fmt:
	go fmt ./...

# Build for multiple platforms
build-all:
	GOOS=linux GOARCH=amd64 go build -o metric-sender-linux main.go
	GOOS=darwin GOARCH=amd64 go build -o metric-sender-mac main.go
	GOOS=windows GOARCH=amd64 go build -o metric-sender.exe main.go

# Test (requires stats-service running)
test:
	@echo "Testing connection to stats-service..."
	@curl -s http://localhost:7000/health > /dev/null && echo "✓ Stats-service is running" || (echo "✗ Stats-service not reachable" && exit 1)
	@echo "Running metric-sender..."
	@go run main.go

# Install to /usr/local/bin
install: build
	sudo cp metric-sender /usr/local/bin/
	@echo "Installed to /usr/local/bin/metric-sender"

# Uninstall from /usr/local/bin
uninstall:
	sudo rm -f /usr/local/bin/metric-sender
	@echo "Uninstalled from /usr/local/bin"
