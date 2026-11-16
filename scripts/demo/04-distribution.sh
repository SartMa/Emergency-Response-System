#!/usr/bin/env bash
# Demo 4 (spec §10 "Data Distribution"): keys spread over the 3 shards by hash.
source "$(dirname "$0")/lib.sh"

step "Create 12 requests and ask the coordinator where each one lives"
printf "   %-44s %-9s %s\n" key partition shard
for i in $(seq 1 12); do
  id=$(new_request food 18.5 73.8)
  curl -s "$COORD/internal/locate/req:$id" | jq -r '"   \(.key) \(.partition|tostring|.+"        "|.[:9]) \(.shard)  (primary \(.primary))"'
done
note "sha1(key) mod 12 picks a partition; the partition map assigns partitions to shards"
pause

step "Key counts per node (primary and replica hold the same keys)"
cluster
note "Live view: $COORD/dashboard"
