#!/usr/bin/env bash
# Control the CLOUD cluster from your laptop, privately, through SSH. Keep this running.
#
#   make cloud-tunnel
#
# Opens two things, both only on your laptop (nothing new is opened on the internet):
#   localhost:16443  -> the cloud Kubernetes API (k3s, port 6443 on the VM), through SSH
#   localhost:8091   -> the cloud storage dashboard / coordinator, via kubectl port-forward
# Then prints the settings that point the existing demo scripts, tests and chaos test at the cloud.
# Ctrl+C closes everything.
set -euo pipefail
cd "$(dirname "$0")/.."

inventory=ansible/inventory.ini
host=$(awk '/ansible_host=/ {for (i = 1; i <= NF; i++) {split($i, kv, "="); if (kv[1] == "ansible_host") print kv[2]}}' "$inventory")
user=$(awk '/ansible_user=/ {for (i = 1; i <= NF; i++) {split($i, kv, "="); if (kv[1] == "ansible_user") print kv[2]}}' "$inventory")
[ -n "$host" ] || { echo "No ansible_host in $inventory (see ansible/README.md)"; exit 1; }
ssh_target="${user:-ubuntu}@$host"

# The cluster's access file is a credential: it goes in ansible/credentials/ (git-ignored).
kubeconfig=ansible/credentials/kubeconfig-cloud
mkdir -p ansible/credentials
echo "Fetching the cloud cluster's kubeconfig from $ssh_target ..."
ssh "$ssh_target" 'sudo cat /etc/rancher/k3s/k3s.yaml' \
  | sed 's#https://127.0.0.1:6443#https://127.0.0.1:16443#' > "$kubeconfig"
chmod 600 "$kubeconfig"

pids=()
cleanup() { kill "${pids[@]}" 2>/dev/null || true; echo; echo "Tunnels closed."; }
trap cleanup EXIT INT TERM

# 1. Kubernetes API through SSH. Local port 16443, not 6443, so it can't clash with anything local.
ssh -N -o ExitOnForwardFailure=yes -L 16443:127.0.0.1:6443 "$ssh_target" &
pids+=($!)
for _ in $(seq 1 20); do
  KUBECONFIG=$kubeconfig kubectl get --raw=/readyz >/dev/null 2>&1 && break
  sleep 0.5
done
KUBECONFIG=$kubeconfig kubectl get --raw=/readyz >/dev/null || { echo "Kubernetes API tunnel did not come up"; exit 1; }

# 2. Dashboard / coordinator on localhost:8091 (8090 is minikube's). Restarted automatically if
#    the coordinator pod restarts (demo 08 does that), because a port-forward dies with its pod.
(
  while true; do
    KUBECONFIG=$kubeconfig kubectl -n erm port-forward svc/coordinator 8091:8000 >/dev/null 2>&1 || true
    sleep 2
  done
) &
pids+=($!)

cat <<EOF

Connected to the cloud cluster at $host. Keep this window open; Ctrl+C closes the tunnels.

  App (public):          http://$host/
  Storage dashboard:     http://localhost:8091/dashboard

In ANOTHER terminal, point everything at the cloud with:

  export KUBECONFIG=$PWD/$kubeconfig GATEWAY=http://$host COORD=http://localhost:8091

then, for example:
  kubectl -n erm get pods                                   # the cloud's pods
  NO_PAUSE=1 ./scripts/demo/06-storage-failure.sh           # kill a storage node, watch it heal
  GATEWAY_URL=\$GATEWAY COORDINATOR_URL=\$COORD uv run pytest tests/integration
  uv run python scripts/chaos.py --gateway \$GATEWAY --coord \$COORD

(Without the export, kubectl and the scripts keep talking to minikube as before.)
EOF
wait
