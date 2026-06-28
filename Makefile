.PHONY: build vet test test-integration test-db tidy clean

build:
	go build ./...

vet:
	go vet ./...

# Unit tests only; integration tests self-skip without CHRONICLE_TEST_DB.
test:
	go test ./...

# Integration tests against a real Postgres. They share one DB, so run serially
# (-p 1). Self-skip without CHRONICLE_TEST_DB. `make test-db` spins a throwaway
# container and runs the whole suite against it.
test-integration:
	go test -p 1 ./...

test-db:
	./scripts/with-test-db.sh go test -p 1 ./...

tidy:
	go mod tidy

clean:
	go clean
	rm -f chronicle chronicle-agent
