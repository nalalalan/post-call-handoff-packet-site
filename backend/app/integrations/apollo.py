from __future__ import annotations

from typing import Any, Dict, List

import httpx

from app.core.config import settings


class ApolloClient:
    base_url = "https://api.apollo.io/api/v1"

    def _headers(self) -> Dict[str, str]:
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-Api-Key": settings.apollo_api_key,
            "Authorization": f"Bearer {settings.apollo_api_key}",
            "Cache-Control": "no-cache",
        }

    async def _post(
        self,
        path: str,
        *,
        json_body: Dict[str, Any] | None = None,
        params: Any = None,
    ) -> Dict[str, Any]:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                f"{self.base_url}{path}",
                headers=self._headers(),
                json=json_body,
                params=params,
            )
            response.raise_for_status()
            parsed = response.json()
            return parsed if isinstance(parsed, dict) else {}

    def _query_params(self, payload: Dict[str, Any]) -> list[tuple[str, Any]]:
        params: list[tuple[str, Any]] = []
        for key, value in payload.items():
            if value is None or value == "":
                continue
            if isinstance(value, list):
                param_key = key if key.endswith("[]") else f"{key}[]"
                for item in value:
                    params.append((param_key, item))
            else:
                params.append((key, value))
        return params

    async def search_people(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            result = await self._post("/mixed_people/api_search", params=self._query_params(payload))
            result["_apollo_endpoint"] = "mixed_people/api_search"
            return result
        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code if exc.response is not None else 0
            if status_code not in {403, 404, 422}:
                raise
            result = await self._post("/mixed_people/search", json_body=payload)
            result["_apollo_endpoint"] = "mixed_people/search"
            result["_apollo_primary_error_status"] = status_code
            return result

    async def enrich_person(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return await self._post("/people/match", params=payload)

    async def bulk_enrich_people(
        self,
        details: List[Dict[str, Any]],
        *,
        reveal_personal_emails: bool = False,
        run_waterfall_email: bool = False,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {}
        if reveal_personal_emails:
            params["reveal_personal_emails"] = "true"
        if run_waterfall_email:
            params["run_waterfall_email"] = "true"
        return await self._post(
            "/people/bulk_match",
            json_body={"details": details},
            params=params or None,
        )

    async def enrich_people_individually(self, payloads: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        for payload in payloads:
            try:
                results.append(await self.enrich_person(payload))
            except Exception as exc:  # pragma: no cover
                results.append({"error": str(exc), "input": payload})
        return results
