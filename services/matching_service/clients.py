"""Adapters: matching-service owns no data. It reads and changes requests and resources only
through the owning services' APIs (database-per-service, DESIGN.md §4.3)."""

import httpx

from services.common.config import REQUEST_SERVICE_URL, RESOURCE_SERVICE_URL
from services.common.errors import Unavailable, raise_for_error_response
from services.common.http import make_client


class _Client:
    def __init__(self, base_url: str, name: str):
        self.http = make_client(base_url, timeout=8.0)
        self.name = name

    async def call(self, method: str, url: str, **kwargs) -> dict:
        try:
            r = await self.http.request(method, url, **kwargs)
        except httpx.TransportError as exc:
            raise Unavailable(f"{self.name} unreachable ({exc.__class__.__name__})", code="DEPENDENCY_UNAVAILABLE")
        raise_for_error_response(r)
        return r.json()

    async def close(self) -> None:
        await self.http.aclose()


class RequestServiceClient(_Client):
    def __init__(self, base_url: str = REQUEST_SERVICE_URL):
        super().__init__(base_url, "request-service")

    async def get(self, request_id: str) -> dict:
        return await self.call("GET", f"/requests/{request_id}")

    async def assign(self, request_id: str, resource_id: str, quantity: int, distance_km: float,
                     if_match: int) -> dict:
        return await self.call(
            "POST", f"/internal/requests/{request_id}/assign",
            json={"resource_id": resource_id, "quantity": quantity, "distance_km": distance_km},
            headers={"If-Match": f'"{if_match}"'},
        )

    async def mark_unmatched(self, request_id: str) -> dict:
        return await self.call("POST", f"/internal/requests/{request_id}/unmatched")

    async def list_unmatched(self, type_: str) -> list[dict]:
        body = await self.call("GET", "/requests", params={"status": "UNMATCHED", "type": type_})
        return body["items"]


class ResourceServiceClient(_Client):
    def __init__(self, base_url: str = RESOURCE_SERVICE_URL):
        super().__init__(base_url, "resource-service")

    async def list_available(self, type_: str) -> list[dict]:
        body = await self.call("GET", "/resources", params={"type": type_, "status": "AVAILABLE"})
        return body["items"]

    async def reserve(self, resource_id: str, request_id: str, quantity: int) -> dict:
        return await self.call("POST", f"/internal/resources/{resource_id}/reserve",
                               json={"request_id": request_id, "quantity": quantity})

    async def release(self, resource_id: str, request_id: str) -> dict:
        return await self.call("POST", f"/internal/resources/{resource_id}/release",
                               json={"request_id": request_id})
