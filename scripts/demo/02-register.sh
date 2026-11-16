#!/usr/bin/env bash
# Demo 2 (spec §10 "Resource Registration"): register volunteers / NGOs / supplies.
source "$(dirname "$0")/lib.sh"

step "POST /resources: three water resources around Kurla, Mumbai"
for spec in \
  '{"type":"water","name":"Volunteer Asha (2 cans)","provider":"VOLUNTEER","quantity":2,"location":{"lat":19.0728,"lon":72.8826,"label":"Kurla"}}' \
  '{"type":"water","name":"NGO tanker MH-01","provider":"NGO","quantity":20,"location":{"lat":19.0596,"lon":72.8295,"label":"Bandra"}}' \
  '{"type":"water","name":"Municipal depot Thane","provider":"SUPPLY","quantity":200,"location":{"lat":19.2183,"lon":72.9781,"label":"Thane"}}'; do
  out=$(call POST /resources "$spec")
  echo "$out" | body_of | jq -c '{id: .id[:8], name, status, availability: .availability.quantity_remaining}'
  note "HTTP $(echo "$out" | status_of)"
done
pause

step "GET /resources?type=water&status=AVAILABLE   (scatter-gather over all 3 shards)"
call GET "/resources?type=water&status=AVAILABLE" | body_of | jq '{count, partial, items: [.items[] | {name, status, remaining: .availability.quantity_remaining, location: .location.label}]}'
