# goqueue developer commands. Works with GNU make (Git Bash on Windows is fine).

GO      ?= go
GOEXE   := $(shell $(GO) env GOEXE)
BIN     := bin
COMPOSE := docker compose -f deploy/docker-compose.yml
DURATION ?= 30s

.PHONY: build test test-race lint fmt vet bench-fsm bench-pg bench-raft chaos chaos-partition up down clean

build:
	$(GO) build -o $(BIN)/ ./cmd/...

test:
	$(GO) test ./...

test-race:
	$(GO) test -race ./...

vet:
	$(GO) vet ./...

fmt:
	gofmt -w .

lint: vet
	@test -z "$$(gofmt -l .)" || (echo "gofmt needed on:"; gofmt -l .; exit 1)
	golangci-lint run ./...

bench-fsm:
	$(GO) test -run '^$$' -bench . -benchmem ./internal/fsm/

# Postgres baseline: starts postgres and a postgres-mode goqueued in Docker,
# then drives it with 16 producers and 32 workers.
bench-pg: build
	$(COMPOSE) up -d --build --wait postgres goqueued-pg
	$(BIN)/goqueue-bench$(GOEXE) load --urls http://127.0.0.1:8080 --producers 16 --workers 32 \
		--duration $(DURATION) --payload 1024 --backend-label pg

# Raft: 3-node cluster in Docker (note: Docker shares one disk, so fsync is shared).
bench-raft: build
	$(COMPOSE) up -d --build n1 n2 n3
	$(BIN)/goqueue-bench$(GOEXE) load --urls http://127.0.0.1:8081,http://127.0.0.1:8082,http://127.0.0.1:8083 \
		--producers 16 --workers 32 --duration $(DURATION) --payload 1024 --backend-label raft-3

# Chaos: spawns a local 3-node cluster as child processes and kills the leader
# every 10 seconds under load. Fails if any acknowledged job is lost.
chaos: build
	$(BIN)/goqueue-bench$(GOEXE) chaos --nodes 3 --binary $(BIN)/goqueued$(GOEXE) \
		--duration 120s --kill-every 10s

chaos-partition: build
	$(BIN)/goqueue-bench$(GOEXE) chaos --nodes 3 --binary $(BIN)/goqueued$(GOEXE) \
		--duration 120s --kill-every 10s --partition

up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down -v

clean:
	rm -rf $(BIN)
