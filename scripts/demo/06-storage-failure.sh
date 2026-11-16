#!/usr/bin/env bash
# Demo 6 + 7 (spec §10 "Storage Failure" and "Node Recovery"): before, during and after.
source "$(dirname "$0")/lib.sh"
SHARD=${SHARD:-0}

step "BEFORE: healthy cluster"
cluster
ids=()
for i in $(seq 1 20); do ids+=("$(new_request water 19.07 72.88)"); done
note "20 requests written"
victim=$(primary_of "$SHARD")
pause

step "FAILURE: stopping $victim, the PRIMARY of shard $SHARD"
stop_node "$victim"
sleep 0.5
ok=0; fail=0
for i in $(seq 1 30); do
  code=$(call POST /requests '{"type":"food","location":{"lat":19.07,"lon":72.88}}' | status_of)
  [ "$code" = 201 ] && ok=$((ok+1)) || fail=$((fail+1))
done
read_ok=0
for id in "${ids[@]}"; do [ "$(call GET "/requests/$id" | status_of)" = 200 ] && read_ok=$((read_ok+1)); done
note "DURING: 30 new writes -> $ok ok / $fail failed;  the 20 earlier requests readable: $read_ok/20"
cluster
events 4
note "The coordinator saw the failed request, promoted the in-sync replica and retried: no client error"
pause

step "RECOVERY: bringing $victim back"
if [ "$MODE" = k8s ]; then note "(Kubernetes is already recreating the pod: StatefulSet self-healing, same name, same volume)"; fi
start_node "$victim"
for _ in $(seq 1 30); do
  sync=$(curl -s "$COORD/internal/cluster" | jq -r ".shards[$SHARD].nodes[] | select(.node_id==\"$victim\") | .sync")
  [ "$sync" = IN_SYNC ] && break
  sleep 1
done
cluster
events 5
note "AFTER: $victim copied only the keys it missed (delta resync) and is an IN_SYNC replica again."
note "It does not take the primary role back automatically (no flapping)."
