"""Hash partitioning (DESIGN.md §5.1, ADR-03).

key --sha1--> one of NUM_PARTITIONS fixed partitions --partition map--> shard

Terminology: our "partition" is a slice of the key space; our "shard" is a replicated group of nodes.

Why 12 fixed partitions instead of `hash mod 3`: if the number of shards ever changes,
`mod N` would move ~75% of keys, while with fixed partitions only whole partitions move
(the minimum possible). The map is a plain table, so a rebalance = editing the table.
"""

import hashlib

NUM_PARTITIONS = 12
NUM_SHARDS = 3


def partition_of(key: str) -> int:
    # sha1, NOT Python's hash(): hash() is randomised per process (PYTHONHASHSEED), so two
    # services would disagree about where a key lives.
    sha1_bytes = hashlib.sha1(key.encode("utf-8")).digest()
    return int.from_bytes(sha1_bytes[:8], "big") % NUM_PARTITIONS


def default_partition_map(num_partitions: int = NUM_PARTITIONS, num_shards: int = NUM_SHARDS) -> dict[int, int]:
    """Round-robin: partition p -> shard p mod num_shards (4 partitions per shard)."""
    return {p: p % num_shards for p in range(num_partitions)}


class Partitioner:
    """Strategy for key placement. The coordinator only depends on `shard_of`, so the
    placement rule can be swapped without touching routing code (OCP)."""

    def __init__(self, partition_map: dict[int, int] | None = None):
        self.partition_map = partition_map or default_partition_map()

    def shard_of(self, key: str) -> int:
        return self.partition_map[partition_of(key)]

    def locate(self, key: str) -> tuple[int, int]:
        partition = partition_of(key)
        return partition, self.partition_map[partition]
