#!/usr/bin/env bash
# Demo 3 (spec §10 "Resource Matching"): async match, on-demand match, cancel compensation.
source "$(dirname "$0")/lib.sh"

step "Register resources: a close volunteer with 2 units, a tanker 5 km away with 20"
small=$(new_resource water "Volunteer (2 units)" 19.0790 72.8790 2)
tanker=$(new_resource water "Tanker (20 units)" 19.0596 72.8295 20)
note "volunteer ${small:0:8}  tanker ${tanker:0:8}"

step "Request 4 units of water at Kurla West (asynchronous: request-service -> RabbitMQ -> matching)"
id=$(new_request water 19.0760 72.8777 4 4)
sleep 1.5
call GET "/requests/$id" | body_of | jq '{status, assignment}'
note "The volunteer is closer but has only 2 units, so the tanker wins (type + location + quantity)"
pause

step "Tanker after the match"
call GET "/resources/$tanker" | body_of | jq '{name, status, availability, allocations}'
pause

step "Cancel the request -> request.cancelled event -> resource-service gives the 4 units back (saga compensation)"
call PUT "/requests/$id/status" '{"status":"CANCELLED","note":"family evacuated"}' | body_of | jq '{status}'
sleep 1
call GET "/resources/$tanker" | body_of | jq '{name, status, availability, allocations}'
pause

step "POST /match/{id}: on-demand matching, same saga, synchronous answer"
# A fresh random spot in the South Pacific on every run, so shelters left by earlier runs
# can't be nearby (the demo stays repeatable without wiping data).
lat=$(awk -v s="$RANDOM" 'BEGIN { srand(s); printf "%.3f", -55 + rand() * 20 }')
lon=$(awk -v s="$RANDOM" 'BEGIN { srand(s); printf "%.3f", -150 + rand() * 40 }')
far=$(new_request shelter "$lat" "$lon")   # nowhere near any shelter
sleep 1
out=$(call POST "/match/$far")
echo "$out" | body_of | jq -c .error
note "HTTP $(echo "$out" | status_of): $(echo "$out" | body_of | jq -r .error.code) (expected NO_RESOURCE_AVAILABLE: nothing within 50 km)"
note "Meanwhile the request is UNMATCHED and retried in the background (2, 4, 8, 16, 32 s)"
pause

step "Register a shelter nearby: nobody calls /match, the waiting request is woken up automatically"
hall=$(new_resource shelter "School hall" "$(echo "$lat + 0.01" | awk '{print $1 + $3}')" "$lon" 50)
for _ in $(seq 1 20); do
  st=$(call GET "/requests/$far" | body_of | jq -r .status)
  [ "$st" = ASSIGNED ] && break
  sleep 0.5
done
call GET "/requests/$far" | body_of | jq '{status, assignment, match_attempts}'
note "resource.registered event -> matching-service woke up UNMATCHED shelter requests within 50 km"
note "POST /match now answers $(call POST "/match/$far" | status_of) NOT_MATCHABLE: it is already assigned"
