"""Adapter: the distributed key-value store, as seen by request- and resource-service.

Services talk ONLY to the coordinator (CLAUDE.md rule); they never know which shard or node
holds a key (spec §4: "The client should not need to know the physical storage location").

This is the Repository pattern's storage port: domain code asks for "get / put with version",
and this class turns that into HTTP calls on the coordinator's /internal/data API.
"""

from dataclasses import dataclass

import httpx

from services.common.config import COORDINATOR_URL
from services.common.errors import GatewayTimeout, NotFound, Unavailable, raise_for_error_response
from services.common.http import make_client
from services.common.retry import retry_unavailable


@dataclass
class Versioned:
    key: str
    value: dict
    version: int


class StorageClient:
    def __init__(self, base_url: str = COORDINATOR_URL):
        self._http = make_client(base_url, timeout=5.0)

    async def close(self) -> None:
        await self._http.aclose()

    async def _call(self, method: str, url: str, **kwargs) -> httpx.Response:
        async def once() -> httpx.Response:
            try:
                response = await self._http.request(method, url, **kwargs)
            except httpx.ConnectError as exc:
                # Could not even connect: the request was never sent, so retrying is safe.
                raise Unavailable(f"Coordinator unreachable: {exc.__class__.__name__}", code="COORDINATOR_UNAVAILABLE")
            except httpx.TransportError as exc:
                # Sent, but no answer (timeout / connection dropped): we don't know whether the
                # write was applied, so we do NOT retry blindly. The caller sees 504 and decides.
                raise GatewayTimeout(f"Coordinator did not answer: {exc.__class__.__name__}", code="COORDINATOR_TIMEOUT") from None
            raise_for_error_response(response)  # 503 from the coordinator -> Unavailable -> retried
            return response

        # ADR-16: ride out a failover or coordinator restart with exponential backoff + jitter.
        return await retry_unavailable(once, f"storage {method} {url}")

    async def get(self, key: str) -> Versioned:
        body = (await self._call("GET", f"/internal/data/{key}")).json()
        return Versioned(body["key"], body["value"], body["version"])

    async def get_or_none(self, key: str) -> Versioned | None:
        try:
            return await self.get(key)
        except NotFound:
            return None

    async def put(self, key: str, value: dict, expected_version: int) -> int:
        """Conditional write (ADR-06). expected_version=0 means "create, must not exist yet".
        Raises VersionConflict if somebody else wrote first. Returns the new version."""
        body = (
            await self._call(
                "PUT", f"/internal/data/{key}", json={"value": value, "expected_version": expected_version}
            )
        ).json()
        return body["version"]

    async def query(self, kind: str, type_: str | None = None, status: str | None = None) -> tuple[list[Versioned], bool]:
        """Scatter-gather over all shards (DDIA ch. 7, local secondary indexes).
        Returns (items, partial): partial=True if some shard could not answer (NFR-11)."""
        params = {"kind": kind}
        if type_:
            params["type"] = type_
        if status:
            params["status"] = status
        body = (await self._call("GET", "/internal/query", params=params)).json()
        items = [Versioned(i["key"], i["value"], i["version"]) for i in body["items"]]
        return items, body["partial"]
