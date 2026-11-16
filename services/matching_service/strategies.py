"""Matching strategies (Strategy pattern, DESIGN.md §6.3). Pure functions of their inputs.

Selected with the MATCH_STRATEGY env var. A new strategy means a new class plus one line in
STRATEGIES; nothing else changes (Open/Closed principle).

All strategies first apply the hard rules (FR-8, FR-15, FR-17):
  * same type (already filtered by the query),
  * enough quantity left for the whole request (we never split a request),
  * within MAX_RADIUS_KM of the person.
Then they differ only in how they order the survivors.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass

from services.common.geo import haversine_km


@dataclass(frozen=True)
class RequestView:
    id: str
    type: str
    lat: float
    lon: float
    quantity: int
    urgency: int


@dataclass(frozen=True)
class ResourceView:
    id: str
    name: str
    lat: float
    lon: float
    quantity_remaining: int
    quantity_total: int


@dataclass(frozen=True)
class Candidate:
    resource: ResourceView
    distance_km: float
    score: float


class MatchingStrategy(ABC):
    name: str = "abstract"

    def rank(self, request: RequestView, resources: list[ResourceView], max_radius_km: float) -> list[Candidate]:
        candidates = []
        for res in resources:
            if res.quantity_remaining < request.quantity:
                continue
            distance = haversine_km(request.lat, request.lon, res.lat, res.lon)
            if distance > max_radius_km:
                continue
            candidates.append(Candidate(res, round(distance, 2), self.score(res, distance)))
        # Lower score = better. Ties are broken by id so the order is deterministic (testable).
        return sorted(candidates, key=lambda c: (c.score, c.resource.id))

    @abstractmethod
    def score(self, resource: ResourceView, distance_km: float) -> float: ...


class NearestStrategy(MatchingStrategy):
    """Closest resource wins. Simple and easy to justify: help arrives fastest."""

    name = "nearest"

    def score(self, resource: ResourceView, distance_km: float) -> float:
        return distance_km


class BalancedStrategy(MatchingStrategy):
    """Distance, penalised by how depleted the provider already is:
        score = distance_km * (1 + load),  load = 1 - remaining/total
    A provider that is almost empty looks up to 2x further away, so requests spread over
    providers instead of draining the nearest one first (it keeps reserve capacity near it)."""

    name = "balanced"

    def score(self, resource: ResourceView, distance_km: float) -> float:
        total = max(resource.quantity_total, 1)
        load = 1 - resource.quantity_remaining / total
        return distance_km * (1 + load)


STRATEGIES: dict[str, type[MatchingStrategy]] = {s.name: s for s in (NearestStrategy, BalancedStrategy)}


def get_strategy(name: str) -> MatchingStrategy:
    try:
        return STRATEGIES[name]()
    except KeyError:
        raise ValueError(f"Unknown MATCH_STRATEGY {name!r}; choose one of {sorted(STRATEGIES)}")
