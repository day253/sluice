.PHONY: all build test bench clean deps proto docker push-docker remote-deploy multipass multipass-clean run-dev run-node1 run-node2 run-node3

APP      := sluice
CMD_DIR  := ./cmd/sluice
OP_DIR   := ./cmd/operator
BUILD    := CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/$(APP)

all: build

# ---- Dependencies ----
deps:
	go mod tidy

# ---- Build ----
build:
	@mkdir -p bin
	$(BUILD) $(CMD_DIR)

build-operator:
	@mkdir -p bin
	CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/sluice-operator $(OP_DIR)

# ---- Test ----
test:
	go test -v -race -count=1 ./...

# ---- Proto ----
proto:
	protoc -I proto \
		--go_out=. --go_opt=module=github.com/day253/sluice \
		--go-grpc_out=. --go-grpc_opt=module=github.com/day253/sluice \
		proto/sluice.proto proto/sluice_internal.proto

# ---- Benchmarks ----
bench:
	go test -bench=. -benchtime=1s -benchmem ./pkg/...

# ---- Docker ----
docker: build
	docker build -t ghcr.io/day253/sluice:latest .

push-docker: docker
	docker push ghcr.io/day253/sluice:latest

# Build and deploy on the fixed remote MicroK8s host.
# Override with DEPLOY_HOST/DEPLOY_USER/DEPLOY_DIR when needed.
remote-deploy:
	./scripts/deploy-remote.sh

# ---- microk8s (local Kubernetes) ----
microk8s-install:
	sudo snap install microk8s --classic
	sudo usermod -aG microk8s $$USER
	microk8s status --wait-ready
	microk8s enable dns registry hostpath-storage
	@echo "Log out and back in for group changes to take effect"

microk8s-deploy: docker
	docker tag ghcr.io/day253/sluice:latest localhost:32000/sluice:latest
	docker push localhost:32000/sluice:latest
	helm upgrade --install sluice ./charts/sluice \
		--set image.repository=localhost:32000/sluice \
		--set image.tag=latest \
		--set image.pullPolicy=Always \
		--set affinity.enabled=false

microk8s-status:
	kubectl get pods,svc,statefulset -l app.kubernetes.io/name=sluice

microk8s-clean:
	helm uninstall sluice 2>/dev/null; true

microk8s-logs:
	kubectl logs -l app.kubernetes.io/name=sluice --tail=50

microk8s-portforward:
	kubectl port-forward svc/sluice 9090:9090

# ---- Helm ----
helm-lint:
	helm lint ./charts/sluice

helm-template:
	helm template sluice ./charts/sluice

# ---- Multipass (legacy) ----
multipass: build
	./multipass/setup.sh

multipass-clean:
	./multipass/teardown.sh

# ---- Run (development single-node) ----
run-dev: build
	./bin/$(APP) \
		--id=node-1 \
		--api=127.0.0.1:8080 \
		--raft=127.0.0.1:7000 \
		--data=./data/node-1 \
		--bootstrap \
		--workers=100 \
		--log-level=debug

# ---- Multi-node demo (separate terminals) ----
run-node1: build
	./bin/$(APP) \
		--id=node-1 \
		--api=127.0.0.1:8081 \
		--raft=127.0.0.1:7001 \
		--data=./data/node-1 \
		--bootstrap \
		--workers=50

run-node2: build
	./bin/$(APP) \
		--id=node-2 \
		--api=127.0.0.1:8082 \
		--raft=127.0.0.1:7002 \
		--data=./data/node-2 \
		--workers=50

run-node3: build
	./bin/$(APP) \
		--id=node-3 \
		--api=127.0.0.1:8083 \
		--raft=127.0.0.1:7003 \
		--data=./data/node-3 \
		--workers=50

# ---- Clean ----
clean:
	rm -rf bin/ data/
