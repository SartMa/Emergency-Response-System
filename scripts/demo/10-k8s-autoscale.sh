#!/usr/bin/env bash
# Demo 10 (beyond the spec): the HorizontalPodAutoscaler adds matching pods under load and
# removes them afterwards. Needs `minikube addons enable metrics-server` and k8s/60-hpa.yaml.
source "$(dirname "$0")/lib.sh"
N=${N:-1500}
[ "$MODE" = k8s ] || { echo "Kubernetes only (MODE=k8s)"; exit 1; }

step "Autoscaler before the load"
if ! $K get hpa matching-service >/dev/null 2>&1; then
  echo "   No HPA found. Run: kubectl apply -f k8s/60-hpa.yaml"; exit 1
fi
$K get hpa matching-service
note "TARGETS shows '<unknown>' until metrics-server has data (about 1 minute after enabling it)"
pause

step "Send $N requests in the background, spread over 6 depots"
lats=(22.57 22.60 22.54 22.62 22.50 22.66)
for i in 0 1 2 3 4 5; do new_resource food "Autoscale depot $i" "${lats[$i]}" 88.36 1000 >/dev/null; done
( seq 0 $((N - 1)) | xargs -P 30 -I{} sh -c '
    lats="22.57 22.60 22.54 22.62 22.50 22.66"; lat=$(echo $lats | cut -d" " -f$(( {} % 6 + 1 )))
    curl -s -o /dev/null -X POST "'"$GATEWAY"'/requests" -H "Content-Type: application/json" \
      -d "{\"type\":\"food\",\"location\":{\"lat\":$lat,\"lon\":88.361}}"' ) &
load=$!

step "Watch the autoscaler (every 10 s for ~2 minutes; CPU is averaged, so it reacts after 15-30 s)"
for _ in $(seq 1 12); do
  # jsonpath, not column parsing: newer kubectl prints TARGETS as "cpu: 12%/50%" (with a space)
  line=$($K get hpa matching-service -o jsonpath='cpu {.status.currentMetrics[0].resource.current.averageUtilization}% of target 50%, desired replicas {.status.desiredReplicas}')
  pods=$($K get pods -l app=matching-service --no-headers | grep -c Running || true)
  echo "   $(date +%H:%M:%S)  $line  (running pods: $pods)"
  sleep 10
done
wait $load 2>/dev/null || true
note "After the load ends, the HPA scales back towards 2 (60 s stabilisation window, then 1 pod per 30 s)."
note "Note: demo 09 scales manually; with the HPA present, it may change the replica count back afterwards."
