#!/usr/bin/env bash
# Demo 5 (spec §10 "Asynchronous Processing"): many requests at once, processed via RabbitMQ.
source "$(dirname "$0")/lib.sh"
N=${N:-100}

step "One medicine depot with 60 units, then $N simultaneous requests for 1 unit each"
depot=$(new_resource medicine "Hospital pharmacy" 28.6139 77.2090 60)
start=$(date +%s%N); since=$(now_utc)
seq 1 "$N" | xargs -P 25 -I{} sh -c "curl -s -o /dev/null -X POST '$GATEWAY/requests' -H 'Content-Type: application/json' \
  -d '{\"type\":\"medicine\",\"urgency\":'\$(( {} % 5 + 1 ))',\"location\":{\"lat\":28.61,\"lon\":77.21}}'"
note "$N requests accepted (201) in $(( ($(date +%s%N) - start) / 1000000 )) ms; matching continues in the background"

step "Watch the queue drain (matching-service pods are competing consumers on matching.q)"
for _ in $(seq 1 20); do
  pending=$(call GET "/requests?status=PENDING&type=medicine" | body_of | jq .count)
  remaining=$(call GET "/resources/$depot" | body_of | jq .availability.quantity_remaining)
  echo "   PENDING=$pending   depot units left=$remaining"
  [ "$pending" = 0 ] && break
  sleep 0.5
done
pause

step "Result: exactly 60 assigned (never more than the depot holds), the rest UNMATCHED"
call GET "/resources/$depot" | body_of | jq '{status, availability, allocations: (.allocations | length)}'
note "Messages handled per matching pod/container:"
per_pod_counts "$since"
