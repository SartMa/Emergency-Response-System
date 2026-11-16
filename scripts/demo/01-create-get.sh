#!/usr/bin/env bash
# Demo 1 (spec §10 "Request Creation"): create an emergency request and retrieve it.
source "$(dirname "$0")/lib.sh"

step "POST /requests: a family in Kurla West needs drinking water (urgency 4, 4 units)"
out=$(call POST /requests '{"type":"water","quantity":4,"urgency":4,"details":"Family of 4, flooded ground floor, no drinking water","location":{"lat":19.0760,"lon":72.8777,"label":"Kurla West"}}')
echo "$out" | body_of | jq '{id, type, status, urgency, quantity, location, version, queued_for_matching}'
note "HTTP $(echo "$out" | status_of): created; it is now queued in RabbitMQ for asynchronous matching"
id=$(echo "$out" | body_of | jq -r .id)
pause

step "GET /requests/$id"
call GET "/requests/$id" | body_of | jq '{id, status, assignment, history}'
note "status may already be ASSIGNED or UNMATCHED: matching happened in the background"

step "Error handling"
note "invalid body  -> HTTP $(call POST /requests '{"type":"pizza"}' | status_of)"
note "unknown id    -> HTTP $(call GET /requests/does-not-exist | status_of)"
