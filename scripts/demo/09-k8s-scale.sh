#!/usr/bin/env bash
# Demo 9 (spec §10 "Kubernetes Scaling"): scale matching-service out; RabbitMQ spreads work over all pods.
source "$(dirname "$0")/lib.sh"
REPLICAS=${REPLICAS:-4}
N=${N:-120}

step "Scale matching-service 2 -> $REPLICAS"
$K scale deployment/matching-service --replicas="$REPLICAS"
$K rollout status deployment/matching-service --timeout=90s
$K get pods -l app=matching-service
sleep 3  # let the new pods connect to RabbitMQ

step "6 food banks across Kolkata, then $N requests spread around them"
lats=(22.57 22.60 22.54 22.62 22.50 22.66)
for i in 0 1 2 3 4 5; do new_resource food "Food bank $i" "${lats[$i]}" 88.36 100 >/dev/null; done
since=$(now_utc)
seq 0 $((N - 1)) | xargs -P 20 -I{} sh -c '
  lats="22.57 22.60 22.54 22.62 22.50 22.66"; lat=$(echo $lats | cut -d" " -f$(( {} % 6 + 1 )))
  curl -s -o /dev/null -X POST "'"$GATEWAY"'/requests" -H "Content-Type: application/json" \
    -d "{\"type\":\"food\",\"location\":{\"lat\":$lat,\"lon\":88.361}}"'
wait_no_pending food 60
note "All $N processed. Messages handled per pod (competing consumers on one queue):"
per_pod_counts "$since"
pause

step "Scale back to 2"
$K scale deployment/matching-service --replicas=2
$K rollout status deployment/matching-service --timeout=60s
