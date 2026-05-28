IMAGE  := ledger-service
TAG    := dev
NS     := ledger-x

.PHONY: dev-up dev-build dev-deploy dev-down test lint

# ── Local Minikube ────────────────────────────────────────────────────────────

dev-up:
	minikube start --cpus=4 --memory=8g
	minikube addons enable ingress
	kubectl create namespace $(NS) --dry-run=client -o yaml | kubectl apply -f -
	helm repo add bitnami https://charts.bitnami.com/bitnami
	helm repo update
	helm upgrade --install postgresql bitnami/postgresql -n $(NS) -f k8s/values/postgresql-local.yaml
	helm upgrade --install kafka        bitnami/kafka        -n $(NS) -f k8s/values/kafka-local.yaml
	@echo "Waiting for PostgreSQL to be ready..."
	kubectl rollout status statefulset/postgresql -n $(NS) --timeout=120s
	@echo "Run migrations: make migrate"

dev-build:
	eval $$(minikube docker-env) && docker build -t $(IMAGE):$(TAG) .

dev-deploy: dev-build
	kubectl apply -f k8s/ -n $(NS)
	kubectl rollout restart deployment/ledger-service -n $(NS)
	kubectl rollout status  deployment/ledger-service -n $(NS) --timeout=60s

dev-down:
	minikube delete

# ── Migrations ────────────────────────────────────────────────────────────────

migrate:
	kubectl run migrate --rm -it --restart=Never \
	  --image=postgres:16-alpine \
	  --env="PGPASSWORD=$$(kubectl get secret ledger-service-secrets -n $(NS) -o jsonpath='{.data.POSTGRES_PASSWORD}' | base64 -d)" \
	  -n $(NS) \
	  -- psql -h postgresql -U ledgerx -d ledgerx -f /dev/stdin < migrations/001_init.sql

# ── Tests ─────────────────────────────────────────────────────────────────────

test:
	go test ./... -race -count=1

test-integration:
	TEST_DATABASE_URL="$(TEST_DATABASE_URL)" go test ./tests/... -race -v -count=1

# ── Code quality ──────────────────────────────────────────────────────────────

lint:
	go vet ./...
