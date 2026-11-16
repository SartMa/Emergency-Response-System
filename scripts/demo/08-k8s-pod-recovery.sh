#!/usr/bin/env bash
# Demo 8 (spec §10 "Kubernetes Recovery"): kill application pods; Kubernetes replaces them,
# and the service keeps answering because the other replica serves meanwhile.
source "$(dirname "$0")/lib.sh"

step "Application pods now"
$K get pods -l 'app in (request-service,matching-service,coordinator)' -o wide | cut -c1-90

victim=$($K get pods -l app=request-service -o jsonpath='{.items[0].metadata.name}')
step "Deleting $victim while sending 40 requests"
$K delete pod "$victim" --wait=false
ok=0
for i in $(seq 1 40); do
  [ "$(call POST /requests '{"type":"food","location":{"lat":12.97,"lon":77.59}}' | status_of)" = 201 ] && ok=$((ok+1))
  sleep 0.1
done
note "$ok/40 requests succeeded during the pod failure (the Service routes to the surviving replica)"

step "Kubernetes created a replacement (Deployment keeps replicas=2)"
$K rollout status deployment/request-service --timeout=60s
$K get pods -l app=request-service
pause

step "Even the coordinator: delete it, Kubernetes restarts it, and it rebuilds the cluster map from heartbeats"
$K delete pod -l app=coordinator --wait=false
$K rollout status deployment/coordinator --timeout=60s
note "port-forward to the old pod died; restart 'make k8s-dashboard' to see the dashboard again"
