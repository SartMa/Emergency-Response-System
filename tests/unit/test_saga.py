"""The matching saga against in-memory fake services that can lose an answer AFTER doing the work
(a timeout). This is the situation a chaos run exposed: the first saga version guessed "failed"
and released quantity for requests that had in fact been assigned."""

import copy

import pytest

from services.common.errors import Conflict, GatewayTimeout, Unavailable, VersionConflict
from services.matching_service.saga import MatchingSaga
from services.matching_service.strategies import NearestStrategy

SPOT = {"lat": 19.0, "lon": 72.8}


class FakeRequests:
    def __init__(self, status="PENDING", assignment=None):
        self.req = {"id": "req-1", "type": "food", "location": SPOT, "quantity": 2, "urgency": 3,
                    "status": status, "assignment": assignment, "version": 1}
        self.lose_answer = set()     # ops that succeed but then time out (answer lost)
        self.cancel_on_assign = False

    async def get(self, request_id):
        return copy.deepcopy(self.req)

    async def assign(self, request_id, resource_id, quantity, distance_km, if_match):
        if self.cancel_on_assign:  # someone cancels the request just before our assign lands
            self.req.update(status="CANCELLED", version=self.req["version"] + 1)
            self.cancel_on_assign = False
        if self.req["version"] != if_match:
            raise VersionConflict("stale")
        if self.req["status"] not in ("PENDING", "UNMATCHED"):
            raise Conflict("not matchable", code="NOT_MATCHABLE")
        self.req.update(status="ASSIGNED", version=self.req["version"] + 1,
                        assignment={"resource_id": resource_id, "quantity": quantity, "distance_km": distance_km})
        if "assign" in self.lose_answer:
            self.lose_answer.discard("assign")
            raise GatewayTimeout("answer lost")
        return copy.deepcopy(self.req)

    async def mark_unmatched(self, request_id):
        self.req.update(status="UNMATCHED", version=self.req["version"] + 1)
        return copy.deepcopy(self.req)


class FakeResources:
    def __init__(self, *ids, quantity=10):
        self.res = {rid: {"id": rid, "name": rid, "type": "food", "location": {"lat": 19.0 + i * 0.01, "lon": 72.8},
                          "status": "AVAILABLE", "allocations": [],
                          "availability": {"quantity_total": quantity, "quantity_remaining": quantity}}
                    for i, rid in enumerate(ids)}
        self.fail_release_before = 0  # release calls that fail before doing anything (503)

    def held(self, rid):
        return [a["request_id"] for a in self.res[rid]["allocations"]]

    async def list_by_type(self, type_):
        return copy.deepcopy(list(self.res.values()))

    async def reserve(self, rid, request_id, quantity):
        r = self.res[rid]
        if request_id not in self.held(rid):  # idempotent per (resource, request)
            r["allocations"].append({"request_id": request_id, "quantity": quantity})
            r["availability"]["quantity_remaining"] -= quantity
        return copy.deepcopy(r)

    async def release(self, rid, request_id):
        if self.fail_release_before:
            self.fail_release_before -= 1
            raise Unavailable("resource-service down")
        r = self.res[rid]
        for a in [a for a in r["allocations"] if a["request_id"] == request_id]:
            r["allocations"].remove(a)
            r["availability"]["quantity_remaining"] += a["quantity"]
        return copy.deepcopy(r)


def saga(requests, resources):
    return MatchingSaga(requests, resources, NearestStrategy(), max_radius_km=50, retry_base_s=0.001)


async def test_lost_assign_answer_is_not_mistaken_for_failure():
    reqs, res = FakeRequests(), FakeResources("r1")
    reqs.lose_answer.add("assign")
    result = await saga(reqs, res).match("req-1")
    assert result.outcome == "matched" and result.resource_id == "r1"
    assert reqs.req["status"] == "ASSIGNED" and res.held("r1") == ["req-1"]  # quantity still held: consistent


async def test_compensation_release_is_retried_until_it_happens():
    reqs, res = FakeRequests(), FakeResources("r1")
    reqs.cancel_on_assign = True
    res.fail_release_before = 2
    result = await saga(reqs, res).match("req-1")
    assert result.outcome == "not_matchable" and result.request_status == "CANCELLED"
    assert res.held("r1") == [] and res.res["r1"]["availability"]["quantity_remaining"] == 10


async def test_redelivery_resumes_from_the_reservation_already_held():
    reqs, res = FakeRequests(), FakeResources("r1", "r2")
    await res.reserve("r2", "req-1", 2)  # left by an attempt that crashed before assigning
    result = await saga(reqs, res).match("req-1")
    assert result.outcome == "matched" and result.resource_id == "r2"  # not the nearer r1: no second reservation
    assert res.held("r1") == [] and res.held("r2") == ["req-1"]


async def test_resume_does_not_release_a_concurrent_sagas_reservation():
    """Race seen in a chaos run: two sagas for the same request hold reservations on r1 and r2.
    The resuming one must not release r2: the other saga may be about to assign there."""
    reqs, res = FakeRequests(), FakeResources("r1", "r2")
    await res.reserve("r1", "req-1", 2)
    await res.reserve("r2", "req-1", 2)
    result = await saga(reqs, res).match("req-1")
    assert result.outcome == "matched" and res.held("r1") == ["req-1"] and res.held("r2") == ["req-1"]
    # once the request is assigned, a later delivery cleans up the one it does not use
    await saga(reqs, res).match("req-1")
    assert res.held(result.resource_id) == ["req-1"]
    other = "r2" if result.resource_id == "r1" else "r1"
    assert res.held(other) == []


async def test_leftover_reservation_of_a_cancelled_request_is_released():
    reqs, res = FakeRequests(status="CANCELLED"), FakeResources("r1")
    await res.reserve("r1", "req-1", 2)
    result = await saga(reqs, res).match("req-1")
    assert result.outcome == "not_matchable" and res.held("r1") == []


async def test_assigned_request_keeps_its_own_reservation_only():
    reqs = FakeRequests(status="ASSIGNED", assignment={"resource_id": "r1", "quantity": 2, "distance_km": 0})
    res = FakeResources("r1", "r2")
    await res.reserve("r1", "req-1", 2)
    await res.reserve("r2", "req-1", 2)  # duplicate saga's leftover
    await saga(reqs, res).match("req-1")
    assert res.held("r1") == ["req-1"] and res.held("r2") == []


async def test_unknown_outcome_that_never_resolves_propagates_for_redelivery():
    reqs, res = FakeRequests(), FakeResources("r1")

    async def always_down(*a, **k):
        raise Unavailable("request-service down")
    reqs.assign = always_down
    with pytest.raises(Unavailable):
        await saga(reqs, res).match("req-1")
    assert res.held("r1") == ["req-1"]  # NOT released on a guess; the redelivery will resume from it
