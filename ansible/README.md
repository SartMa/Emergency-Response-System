# Cloud deployment (Ansible + k3s on one VM)

One command turns a fresh Ubuntu VM into a running copy of the whole system: Kubernetes (k3s),
the 5 service images built on the VM, RabbitMQ, 3 shards × 2 storage nodes, Ingress, HPA.

## 1. The VM (Oracle Cloud Always Free, or any Ubuntu 22.04/24.04 VM)
- Image **Canonical Ubuntu 24.04**, shape **VM.Standard.A1.Flex** (ARM), 4 OCPU / 24 GB (2 / 12 also works).
- Your SSH public key (`~/.ssh/id_ed25519.pub`) added at creation.
- In the VCN's **Security List**, an ingress rule for **TCP 80** from `0.0.0.0/0` (SSH 22 is there by default).
  The VM's own firewall is opened by the playbook.

## 2. Deploy
```bash
cp ansible/inventory.ini.example ansible/inventory.ini   # put the VM's public IP in it
make cloud-deploy                                        # ~10 min the first time
```
The playbook ends by checking, from your machine, that `http://<ip>/` and the API answer.
Re-run `make cloud-deploy` after committing changes: it deploys **what is committed** (`git archive HEAD`).

## 3. Use it
- App: `http://<ip>/` (public)
- Shell on the VM: `make cloud-ssh`, then `sudo k3s kubectl -n erm get pods`
- **Everything else from your laptop: `make cloud-tunnel`** (keep it running). It opens, through SSH only:
  - `localhost:16443`: the cloud Kubernetes API, so your local `kubectl` can control the cloud cluster
  - `localhost:8091`: the cloud storage dashboard (`/dashboard`); 8090 stays minikube's

  and prints one `export KUBECONFIG=… GATEWAY=… COORD=…` line. Run it in another terminal, and the
  existing demo scripts, integration tests and chaos test work against the cloud **unchanged**:
  ```bash
  NO_PAUSE=1 ./scripts/demo/06-storage-failure.sh
  GATEWAY_URL=$GATEWAY COORDINATOR_URL=$COORD uv run pytest tests/integration
  uv run python scripts/chaos.py --gateway $GATEWAY --coord $COORD
  ```
  Without that export, `kubectl` and the scripts keep talking to minikube.
- Just the dashboard: `make cloud-dashboard`, then http://localhost:8091/dashboard

## What the roles do
| Role | Does |
|---|---|
| `common` | Docker, base packages; removes Oracle's blanket iptables REJECT rules (they block port 80 and break pod networking) |
| `k3s` | Installs k3s: Kubernetes plus Traefik (Ingress), metrics-server (HPA) and local-path storage (PVCs) |
| `app` | `git archive` → VM, builds the 5 images on the VM (ARM or x86), imports them into k3s, generates the RabbitMQ password, applies `k8s/`, waits for rollout |
| `smoke_test` | Checks the landing page and API over the public IP |

## Security
- Only ports 22 and 80 are open (Oracle security list). The storage dashboard, `/internal/*` and the RabbitMQ UI are reachable only through SSH.
- The RabbitMQ password is generated into `ansible/credentials/` (git-ignored), not the demo one from git.
- The API has no login (out of scope, NFR-12): anyone with the IP can create requests. Fine for a demo; delete the VM afterwards.

## Troubleshooting
| Symptom | Fix |
|---|---|
| Oracle: "Out of capacity" creating the VM | Retry later, or another availability domain |
| `make cloud-deploy` hangs at "Gathering Facts" | SSH: check IP/user in `inventory.ini`, and that your key was added to the VM |
| Smoke test fails but pods run | Port 80 missing in Oracle's Security List |
| `Ansible requires the locale encoding to be UTF-8` | Use `make cloud-deploy` (it sets `LC_ALL=C.UTF-8`) |

## Cost
Oracle Always Free: $0 as long as only Always Free resources are used. On other clouds the VM is billed while it **exists**, working or not: delete it when done.
