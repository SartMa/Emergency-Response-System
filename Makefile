# One-word commands for the project. `make help` lists them.
SERVICES := coordinator storage_node request_service resource_service matching_service
K := kubectl -n erm

.PHONY: help test unit chaos up down logs k8s-start k8s-build k8s-deploy k8s-status k8s-gateway k8s-dashboard k8s-rabbitmq k8s-delete cloud-deploy cloud-dashboard cloud-ssh

help:            ## list targets
	@grep -E '^[a-z0-9-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-15s %s\n", $$1, $$2}'

# ---------------------------------------------------------------- local (docker compose)
up:              ## build + start everything locally (API :8080, dashboard :8090/dashboard)
	docker compose up -d --build

down:            ## stop local stack and DELETE its data volumes
	docker compose down -v

logs:            ## follow logs of the app services
	docker compose logs -f request-service resource-service matching-service coordinator

unit:            ## unit tests (no Docker needed)
	uv run pytest tests/unit -q

test:            ## unit + integration tests (needs `make up` or k8s port-forwards)
	uv run pytest -q

chaos:           ## 2 min of traffic + random storage-node kills, then checks 6 guarantees (MODE=compose for local)
	uv run python scripts/chaos.py --mode $${MODE:-k8s} --report chaos-report.json

# ---------------------------------------------------------------- Kubernetes (minikube)
k8s-start:       ## start minikube with enough resources + ingress and metrics-server (for the HPA) addons
	minikube start --cpus=4 --memory=6g
	minikube addons enable ingress
	minikube addons enable metrics-server

k8s-build:       ## build images on the host, then load them into minikube (any runtime; no registry)
	for s in $(SERVICES); do \
		docker build -q --build-arg SERVICE=$$s -t erm/$$s:dev . || exit 1; \
		minikube image load erm/$$s:dev || exit 1; done

k8s-deploy:      ## apply all manifests and wait until pods are ready
	kubectl apply -f k8s/
	$(K) rollout status statefulset/rabbitmq --timeout=180s
	$(K) rollout status deployment/coordinator --timeout=120s
	for s in 0 1 2; do $(K) rollout status statefulset/shard-$$s --timeout=120s; done
	for d in request-service resource-service matching-service; do $(K) rollout status deployment/$$d --timeout=120s; done

k8s-status:      ## pods, services, ingress
	$(K) get pods -o wide
	$(K) get svc,ingress

k8s-gateway:     ## expose the Ingress on localhost:8080 (keep running)
	$(K) port-forward -n ingress-nginx svc/ingress-nginx-controller 8080:80 2>/dev/null || \
		kubectl port-forward -n ingress-nginx svc/ingress-nginx-controller 8080:80

k8s-dashboard:   ## cluster dashboard on localhost:8090/dashboard (keep running)
	$(K) port-forward svc/coordinator 8090:8000

k8s-rabbitmq:    ## RabbitMQ management UI on localhost:15673 (user erm)
	$(K) port-forward svc/rabbitmq 15673:15672

k8s-delete:      ## remove everything from the cluster AND wipe all stored data
	kubectl delete namespace erm
	# minikube's hostpath provisioner names a volume's folder after namespace + claim, and does not
	# remove it: without this line the next deploy silently comes back with all the OLD data.
	minikube ssh -- "sudo rm -rf /tmp/hostpath-provisioner/erm"

# ---------------------------------------------------------------- cloud VM (Ansible + k3s; see ansible/README.md)
# user@ip of the VM, read from ansible/inventory.ini
CLOUD := $(shell awk '/ansible_host=/ {for (i = 1; i <= NF; i++) {split($$i, kv, "="); if (kv[1] == "ansible_host") h = kv[2]; if (kv[1] == "ansible_user") u = kv[2]}} END {if (h) print u "@" h}' ansible/inventory.ini 2>/dev/null)

cloud-deploy:    ## deploy (or update) everything on the cloud VM in ansible/inventory.ini
	cd ansible && LC_ALL=C.UTF-8 uv run ansible-playbook site.yml

cloud-dashboard: ## storage dashboard of the cloud VM on localhost:8090/dashboard (SSH tunnel; keep running)
	ssh -L 8090:localhost:8090 $(CLOUD) 'sudo k3s kubectl -n erm port-forward svc/coordinator 8090:8000'

cloud-ssh:       ## open a shell on the cloud VM (then: sudo k3s kubectl -n erm get pods)
	ssh $(CLOUD)
