GO ?= go

# VERSION is the exact git tag at HEAD (release builds); dev builds with no tag
# at HEAD fall back to dev-<short-commit>. Override with `make VERSION=...`.
VERSION ?= $(shell git describe --tags --exact-match 2>/dev/null || echo "dev-$$(git rev-parse --short HEAD 2>/dev/null || echo unknown)")
LDFLAGS ?= -X github.com/ncode/chronicle/internal/version.Version=$(VERSION)
DIST_DIR ?= dist
SHA256 := $(shell command -v sha256sum >/dev/null 2>&1 && echo "sha256sum" || echo "shasum -a 256")

# The agent follows ncode/facts' release tier (incl plan9); the server adds the
# Postgres-capable subset (excl plan9).
AGENT_TARGETS  ?= linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64 windows/arm64 freebsd/amd64 freebsd/arm freebsd/arm64 openbsd/amd64 openbsd/arm openbsd/arm64 netbsd/amd64 netbsd/arm netbsd/arm64 dragonfly/amd64 illumos/amd64 plan9/amd64
SERVER_TARGETS ?= linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64 windows/arm64 freebsd/amd64 freebsd/arm freebsd/arm64 openbsd/amd64 openbsd/arm openbsd/arm64 netbsd/amd64 netbsd/arm netbsd/arm64 dragonfly/amd64 illumos/amd64

.PHONY: build vet test race bench test-integration test-db cross-compile dist tidy clean

build:
	$(GO) build -ldflags '$(LDFLAGS)' -o chronicle ./cmd/chronicle
	$(GO) build -ldflags '$(LDFLAGS)' -o chronicle-agent ./cmd/chronicle-agent

vet:
	$(GO) vet ./...

# Unit tests; integration tests self-skip without CHRONICLE_TEST_DB.
test:
	$(GO) test ./...

# Race detector over the concurrency-sensitive packages.
race:
	$(GO) test -race ./internal/store ./internal/ingest

# Benchmarks (CPU/alloc floors). Store benchmarks self-skip without
# CHRONICLE_TEST_DB; wrap in scripts/with-test-db.sh to include them.
bench:
	$(GO) test -run '^$$' -bench=. -benchmem ./internal/store ./internal/ingest ./internal/wire

# Integration tests against a real Postgres (CHRONICLE_TEST_DB). MUST run with
# -p 1: DB-backed tests isolate via TRUNCATE on shared tables, so parallel
# packages would clobber each other's rows.
test-integration:
	$(GO) test -p 1 -timeout 120s ./...

# Spin a throwaway Postgres and run the full suite against it. -p 1 for the same
# TRUNCATE-based isolation reason as test-integration.
test-db:
	./scripts/with-test-db.sh $(GO) test -p 1 -timeout 120s ./...

# Build both binaries for every supported os/arch (build-only check).
cross-compile:
	@set -e; \
	for target in $(AGENT_TARGETS); do \
		echo "agent  $$target"; \
		CGO_ENABLED=0 GOOS=$${target%/*} GOARCH=$${target#*/} $(GO) build -o /dev/null ./cmd/chronicle-agent; \
	done; \
	for target in $(SERVER_TARGETS); do \
		echo "server $$target"; \
		CGO_ENABLED=0 GOOS=$${target%/*} GOARCH=$${target#*/} $(GO) build -o /dev/null ./cmd/chronicle; \
	done

# dist builds checksummed release archives <binary>-$(VERSION)-<os>-<arch> for
# every supported os/arch. The version is embedded (internal/version.Version)
# and reported by `<binary> -version`.
dist:
	@set -e; \
	mkdir -p $(DIST_DIR); \
	build_one() { \
		bin=$$1; cmd=$$2; goos=$$3; goarch=$$4; \
		name="$$bin-$(VERSION)-$$goos-$$goarch"; \
		out=$$bin; if [ "$$goos" = windows ]; then out=$$bin.exe; fi; \
		staging="$(DIST_DIR)/$$name"; rm -rf "$$staging"; mkdir -p "$$staging"; \
		echo "building $$name"; \
		CGO_ENABLED=0 GOOS=$$goos GOARCH=$$goarch $(GO) build -trimpath -ldflags '$(LDFLAGS)' -o "$$staging/$$out" ./$$cmd; \
		if [ "$$goos" = windows ]; then \
			rm -f "$(DIST_DIR)/$$name.zip"; (cd $(DIST_DIR) && zip -q -r "$$name.zip" "$$name"); \
		else \
			tar -czf "$(DIST_DIR)/$$name.tar.gz" -C $(DIST_DIR) "$$name"; \
		fi; \
		rm -rf "$$staging"; \
	}; \
	for target in $(SERVER_TARGETS); do build_one chronicle cmd/chronicle "$${target%/*}" "$${target#*/}"; done; \
	for target in $(AGENT_TARGETS);  do build_one chronicle-agent cmd/chronicle-agent "$${target%/*}" "$${target#*/}"; done; \
	(cd $(DIST_DIR) && rm -f SHA256SUMS && $(SHA256) $$(ls chronicle*-$(VERSION)-*.tar.gz chronicle*-$(VERSION)-*.zip 2>/dev/null) > SHA256SUMS); \
	cat $(DIST_DIR)/SHA256SUMS

tidy:
	$(GO) mod tidy

clean:
	$(GO) clean
	rm -f chronicle chronicle-agent
	find . -name '*.test' -type f -delete
	rm -rf $(DIST_DIR)
