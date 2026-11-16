# Shared helpers for the demo scripts. Source it: `source "$(dirname "$0")/lib.sh"`
#
#   MODE=k8s      (default) Kubernetes; needs `make k8s-gateway` and `make k8s-dashboard` running
#   MODE=compose  docker compose; gateway :8080, coordinator :8090
#   NO_PAUSE=1    run without waiting for [enter] between steps

set -euo pipefail

GATEWAY=${GATEWAY:-http://localhost:8080}
COORD=${COORD:-http://localhost:8090}
MODE=${MODE:-k8s}
K="kubectl -n erm"

bold=$(tput bold 2>/dev/null || true); dim=$(tput dim 2>/dev/null || true); reset=$(tput sgr0 2>/dev/null || true)

step()  { echo; echo "${bold}== $*${reset}"; }
note()  { echo "${dim}   $*${reset}"; }
pause() { [ -n "${NO_PAUSE:-}" ] || read -rp "   [enter] to continue " _; }

# curl wrapper that prints "HTTP <code>" after the body. Usage: call POST /requests '{json}'
call() {
  local method=$1 path=$2 body=${3:-}
  if [ -n "$body" ]; then
    curl -s -w '\n%{http_code}' -X "$method" "$GATEWAY$path" -H 'Content-Type: application/json' -d "$body"
  else
    curl -s -w '\n%{http_code}' -X "$method" "$GATEWAY$path"
  fi
}
# Split call() output: body | status
body_of()   { sed '$d'; }
status_of() { tail -n1; }

cluster() {
  curl -s "$COORD/internal/cluster" | jq -r '.shards[] |
    "   shard \(.shard_id) [\(if .available then "available" else "UNAVAILABLE" end)]  " +
    ([.nodes[] | "\(.node_id) \(.role)/\(.health)/\(.sync) keys=\(.keys)"] | join("   "))'
}
events() { curl -s "$COORD/internal/cluster" | jq -r ".events[:${1:-6}][] | \"   \(.age_s)s ago  \(.message)\""; }
primary_of() { curl -s "$COORD/internal/cluster" | jq -r ".shards[$1].primary"; }

new_request() {  # new_request <type> <lat> <lon> [quantity] [urgency] -> prints id
  call POST /requests "$(jq -nc --arg t "$1" --argjson lat "$2" --argjson lon "$3" \
      --argjson q "${4:-1}" --argjson u "${5:-3}" \
      '{type:$t, location:{lat:$lat, lon:$lon}, quantity:$q, urgency:$u, details:"demo"}')" | body_of | jq -r .id
}
new_resource() {  # new_resource <type> <name> <lat> <lon> <quantity> -> prints id
  call POST /resources "$(jq -nc --arg t "$1" --arg n "$2" --argjson lat "$3" --argjson lon "$4" --argjson q "$5" \
      '{type:$t, name:$n, location:{lat:$lat, lon:$lon}, quantity:$q}')" | body_of | jq -r .id
}

now_utc() { date -u +%Y-%m-%dT%H:%M:%SZ; }

# How many match commands each matching pod/container handled since <rfc3339 time>.
per_pod_counts() {
  if [ "$MODE" = k8s ]; then
    $K logs -l app=matching-service --tail=-1 --prefix --since-time="$1" 2>/dev/null
  else
    docker compose logs --since "$1" matching-service 2>/dev/null
  fi | grep "match command done" | cut -d' ' -f1 | sort | uniq -c
}

wait_no_pending() {  # wait_no_pending <type> [max seconds]
  for _ in $(seq 1 $(( ${2:-30} * 2 ))); do
    [ "$(call GET "/requests?status=PENDING&type=$1" | body_of | jq .count)" = 0 ] && return 0
    sleep 0.5
  done
}

stop_node() {
  if [ "$MODE" = k8s ]; then
    $K delete pod "$1" --wait=false >/dev/null   # the StatefulSet will recreate it automatically
  else
    docker compose stop -t 0 "$1" >/dev/null 2>&1
  fi
}
start_node() {
  if [ "$MODE" = k8s ]; then
    $K wait --for=condition=Ready "pod/$1" --timeout=90s >/dev/null
  else
    docker compose start "$1" >/dev/null 2>&1
  fi
}
