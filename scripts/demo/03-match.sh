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
far=$(new_request shelter 10.0 10.0)   # nowhere near any shelter
sleep 1
out=$(call POST "/match/$far")
echo "$out" | body_of | jq -c .error
note "HTTP $(echo "$out" | status_of): NO_RESOURCE_AVAILABLE (nothing within 50 km)"
hall=$(new_resource shelter "School hall" 10.01 10.01 50)
out=$(call POST "/match/$far")
echo "$out" | body_of | jq .
note "HTTP $(echo "$out" | status_of): matched after the shelter was registered"
