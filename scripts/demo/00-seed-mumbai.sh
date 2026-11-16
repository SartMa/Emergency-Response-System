#!/usr/bin/env bash
# Demo 0: realistic data around Mumbai, so the landing page map (http://localhost:8080) has
# something to show. Safe to run more than once (it only adds data).
source "$(dirname "$0")/lib.sh"

step "Resources: NGOs, volunteers and supply depots across Mumbai"
while IFS='|' read -r type name provider qty lat lon label; do
  out=$(call POST /resources "$(jq -nc --arg t "$type" --arg n "$name" --arg p "$provider" --argjson q "$qty" \
        --argjson lat "$lat" --argjson lon "$lon" --arg l "$label" \
        '{type:$t, name:$n, provider:$p, quantity:$q, location:{lat:$lat, lon:$lon, label:$l}}')")
  echo "   $(echo "$out" | status_of)  $name ($type x $qty, $label)"
done <<'EOF'
water|Municipal water tanker MH-01|SUPPLY|40|19.0596|72.8295|Bandra
water|Volunteer Asha (cans)|VOLUNTEER|3|19.0728|72.8826|Kurla
food|Akshaya food kitchen|NGO|60|19.0176|72.8562|Dadar
food|Gurudwara langar|NGO|80|19.1197|72.9058|Powai
medicine|KEM hospital pharmacy|SUPPLY|25|19.0026|72.8416|Parel
medicine|Dr. Mehta (volunteer doctor)|VOLUNTEER|4|19.1136|72.8697|Andheri
transportation|Ambulance 108|SUPPLY|2|19.0330|73.0297|Navi Mumbai
transportation|Volunteer boat team|VOLUNTEER|3|19.0600|72.8900|Kurla creek
shelter|BMC school hall|NGO|120|19.0430|72.8640|Sion
shelter|Community centre Thane|NGO|90|19.2183|72.9781|Thane
EOF

step "Requests from flooded neighbourhoods (matched asynchronously)"
while IFS='|' read -r type qty urgency lat lon label; do
  out=$(call POST /requests "$(jq -nc --arg t "$type" --argjson q "$qty" --argjson u "$urgency" \
        --argjson lat "$lat" --argjson lon "$lon" --arg l "$label" \
        '{type:$t, quantity:$q, urgency:$u, location:{lat:$lat, lon:$lon, label:$l}, details:"seeded demo request"}')")
  echo "   $(echo "$out" | status_of)  $type x $qty, urgency $urgency, $label"
done <<'EOF'
water|4|4|19.0760|72.8777|Kurla West
water|2|3|19.0544|72.8402|Mahim
water|6|5|19.0660|72.8990|Chembur
food|10|3|19.0390|72.8619|Sion
food|5|2|19.1075|72.8263|Juhu
medicine|1|5|19.0100|72.8430|Lower Parel
medicine|2|4|19.1180|72.8480|Versova
transportation|1|5|19.0700|72.8850|Kurla station
shelter|25|4|19.0500|72.8850|Dharavi
shelter|8|3|19.1950|72.9700|Thane West
food|30|2|19.6967|72.7655|Palghar (over 50 km from any kitchen)
EOF

sleep 2
step "Result"
call GET "/requests?limit=11" | body_of | jq -r '.items[] | "   \(.status | .+"          " | .[:10]) \(.type) x \(.quantity) at \(.location.label)" + (if .assignment then " -> \(.assignment.distance_km) km" else "" end)'
note "Open http://localhost:8080 and press 'Mumbai' on the map."
