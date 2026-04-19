from __future__ import annotations

from pathlib import Path
from typing import Any

import requests
from requests import Response, Session


class CrowdfarmingClient:
    def __init__(
        self,
        base_url: str,
        token: str,
        timeout_seconds: int,
        max_retries: int,
        session: Session | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.session = session or requests.Session()
        self.session.headers.update(
            {
                "Authorization": token,
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "es-ES,es;q=0.9",
                "Origin": "https://farmer.crowdfarming.com",
                "X-Origin-CF": "https://farmer.crowdfarming.com",
                "Referer": "https://farmer.crowdfarming.com/",
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/26.5 Safari/605.1.15"
                ),
            }
        )

    def get_payouts(self, farmer_id: str, start_date: str, end_date: str) -> list[dict[str, Any]]:
        url = f"{self.base_url}/services/payout/v1/payouts"
        start = 0
        limit = 100
        collected: list[dict[str, Any]] = []

        while True:
            params = {
                "farmerId": farmer_id,
                "startDate": start_date,
                "endDate": end_date,
                "isAuth": "true",
                "start": str(start),
                "limit": str(limit),
            }
            response = self._request("GET", url, params=params)
            payload = response.json()
            page_items, total_count, page_limit = self._extract_payout_page(payload)
            collected.extend(page_items)

            if not page_items:
                break
            if total_count is not None and len(collected) >= total_count:
                break

            start += page_limit or len(page_items)
            if page_limit == 0:
                break

        return collected

    def download_transactions_xls(self, payout_id: str) -> tuple[bytes, str]:
        url = f"{self.base_url}/services/payout/v1/payouts/{payout_id}/transactions-xls"
        response = self._request("GET", url, params={"isAuth": "true"})
        content_type = response.headers.get("Content-Type", "")

        if "application/json" in content_type:
            payload = response.json()
            signed_url = _extract_transactions_xls_url(payload)
            file_response = self._request("GET", signed_url)
            content = file_response.content
            if not content:
                raise ValueError(f"Empty XLSX response for payout {payout_id}")
            filename = _filename_from_headers(file_response) or f"payout_{payout_id}.xlsx"
        else:
            content = response.content
            if not content:
                raise ValueError(f"Empty XLSX response for payout {payout_id}")
            filename = _filename_from_headers(response) or f"payout_{payout_id}.xlsx"

        if not filename.lower().endswith(".xlsx"):
            filename = f"{Path(filename).stem}.xlsx"
        return content, filename

    def _request(self, method: str, url: str, **kwargs: Any) -> Response:
        for attempt in range(self.max_retries + 1):
            try:
                response = self.session.request(
                    method,
                    url,
                    timeout=self.timeout_seconds,
                    **kwargs,
                )
                if response.status_code in {401, 403}:
                    response.raise_for_status()
                if response.status_code >= 500 and attempt < self.max_retries:
                    continue
                response.raise_for_status()
                return response
            except requests.RequestException:
                if attempt >= self.max_retries:
                    raise
        raise RuntimeError("Unexpected retry loop termination")

    @staticmethod
    def _extract_payout_page(payload: Any) -> tuple[list[dict[str, Any]], int | None, int | None]:
        if isinstance(payload, list):
            items = [item for item in payload if isinstance(item, dict)]
            return items, len(items), len(items)

        if isinstance(payload, dict):
            data = payload.get("data")
            if isinstance(data, dict):
                list_candidate = data.get("list")
                if isinstance(list_candidate, list):
                    items = [item for item in list_candidate if isinstance(item, dict)]
                    total_count = data.get("totalCount")
                    limit = data.get("limit")
                    return (
                        items,
                        total_count if isinstance(total_count, int) else None,
                        limit if isinstance(limit, int) else len(items),
                    )

            candidates = [payload.get("data"), payload.get("items"), payload.get("results"), payload.get("payouts")]
            for candidate in candidates:
                if isinstance(candidate, list):
                    items = [item for item in candidate if isinstance(item, dict)]
                    return items, len(items), len(items)

        raise ValueError(f"Unsupported payouts response format: {type(payload).__name__}")


def _filename_from_headers(response: Response) -> str | None:
    content_disposition = response.headers.get("Content-Disposition", "")
    marker = "filename="
    if marker not in content_disposition:
        return None

    filename = content_disposition.split(marker, 1)[1].strip().strip('"')
    return Path(filename).name or None


def _extract_transactions_xls_url(payload: Any) -> str:
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, dict):
            url = data.get("transactionsXlsUrl")
            if isinstance(url, str) and url.strip():
                return url
        url = payload.get("transactionsXlsUrl")
        if isinstance(url, str) and url.strip():
            return url

    raise ValueError("transactions-xls response did not include a downloadable URL")
