.PHONY: all build test clean deps run-dev run-node1 run-node2 run-node3

APP     := rate-limiter
CMD_DIR := ./cmd/rate-limiter
BUILD   := CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/$(APP)

all: build

# ---- Dependencies ----
deps:
	@export PATH=$(HOME)/go/bin:$$PATH && \
	export GOROOT=$(HOME)/go && \
	export GOPATH=$(HOME)/go/packages && \
	go mod tidy

# ---- Build ----
build:
	@mkdir -p bin
	$(BUILD) $(CMD_DIR)

# ---- Test ----
test:
	@export PATH=$(HOME)/go/bin:$$PATH && \
	export GOROOT=$(HOME)/go && \
	export GOPATH=$(HOME)/go/packages && \
	go test -v -race -count=1 ./...

# ---- Run (development single-node) ----
run-dev: build
	./bin/$(APP) \
		--id=node-1 \
		--http=127.0.0.1:8080 \
		--raft=127.0.0.1:7000 \
		--data=./data/node-1 \
		--bootstrap \
		--workers=100 \
		--log-level=debug

# ---- Multi-node demo (separate terminals) ----
run-node1: build
	./bin/$(APP) \
		--id=node-1 \
		--http=127.0.0.1:8081 \
		--raft=127.0.0.1:7001 \
		--data=./data/node-1 \
		--bootstrap \
		--workers=50

run-node2: build
	./bin/$(APP) \
		--id=node-2 \
		--http=127.0.0.1:8082 \
		--raft=127.0.0.1:7002 \
		--data=./data/node-2 \
		--workers=50

run-node3: build
	./bin/$(APP) \
		--id=node-3 \
		--http=127.0.0.1:8083 \
		--raft=127.0.0.1:7003 \
		--data=./data/node-3 \
		--workers=50

# ---- Clean ----
clean:
	rm -rf bin/ data/
