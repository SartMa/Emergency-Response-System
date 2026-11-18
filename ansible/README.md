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
- App: `http://<ip>/`
- Storage dashboard (deliberately not public): `make cloud-dashboard`, then http://localhost:8090/dashboard
- Shell: `make cloud-ssh`, then `sudo k3s kubectl -n erm get pods`
- Demo scripts against the cloud: `GATEWAY=http://<ip> COORD=http://localhost:8090 ./scripts/demo/01-create-get.sh`
  (with `make cloud-dashboard` running for COORD). Scripts that kill pods use local `kubectl`, so
  run those on the VM or point kubectl at it.

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
