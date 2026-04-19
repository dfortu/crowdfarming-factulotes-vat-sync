from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests
from requests import Response, Session


XLSX_MIME_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
ACCOUNTING_XLSX_FORMATS = {"ares", "resumen_simple", "a3", "resumen", "gestion"}
ACCOUNTING_XML_FORMATS = {"hispatec"}


@dataclass(frozen=True)
class AccountingExport:
    formato: str
    extension: str
    content: bytes


class FactulotesClient:
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
                "Authorization": f"Bearer {token}",
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en",
                "Origin": "https://factulotes.es",
                "Referer": "https://factulotes.es/",
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/26.5 Safari/605.1.15"
                ),
                "X-Origin-CF": "https://app.farmeneur.com",
                "X-Requested-With": "XMLHttpRequest",
            }
        )

    def upload_file(self, file_path: Path) -> dict[str, Any]:
        url = f"{self.base_url}/archivos"
        for attempt in range(self.max_retries + 1):
            try:
                with file_path.open("rb") as file_handle:
                    response = self.session.post(
                        url,
                        params={"carpeta": "uploads"},
                        files={
                            "file": (
                                file_path.name,
                                file_handle,
                                XLSX_MIME_TYPE,
                            )
                        },
                        timeout=self.timeout_seconds,
                    )
                if response.status_code in {401, 403}:
                    response.raise_for_status()
                if response.status_code >= 500 and attempt < self.max_retries:
                    continue
                response.raise_for_status()
                return _decode_response(response)
            except requests.RequestException:
                if attempt >= self.max_retries:
                    raise
        raise RuntimeError("Unexpected retry loop termination")

    def get_contadores(self) -> list[dict[str, Any]]:
        url = f"{self.base_url}/contadores"
        response = self._request("GET", url, params={"$limit": -1})
        payload = response.json()
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        if isinstance(payload, dict):
            data = payload.get("data")
            if isinstance(data, list):
                return [item for item in data if isinstance(item, dict)]
        raise ValueError("Unexpected contadores response format")

    def get_lotes(self) -> list[dict[str, Any]]:
        url = f"{self.base_url}/lotes"
        response = self._request("GET", url, params={"$limit": -1})
        payload = response.json()
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        if isinstance(payload, dict):
            data = payload.get("data")
            if isinstance(data, list):
                return [item for item in data if isinstance(item, dict)]
        raise ValueError("Unexpected lotes response format")

    def generate_contabilidad(self, lote_ids: list[int], formato: str) -> AccountingExport:
        url = f"{self.base_url}/contabilidad/"
        response = self._request("POST", url, json={"lote": lote_ids, "formato": formato})
        payload = response.json()
        if not isinstance(payload, str):
            raise ValueError("Unexpected contabilidad response format")

        if formato in ACCOUNTING_XML_FORMATS:
            return AccountingExport(formato=formato, extension="xml", content=payload.encode("utf-8"))
        if formato in ACCOUNTING_XLSX_FORMATS:
            return AccountingExport(
                formato=formato,
                extension="xlsx",
                content=payload.encode("latin-1"),
            )
        raise ValueError(f"Unsupported contabilidad format: {formato}")

    def create_lote(self, nombre: str, contador: int, fecha: str) -> dict[str, Any]:
        url = f"{self.base_url}/lotes"
        payload = {"nombre": nombre, "contador": contador, "fecha": fecha}
        response = self._request("POST", url, json=payload)
        return _decode_response(response)

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


def _decode_response(response: Response) -> dict[str, Any]:
    try:
        payload = response.json()
        if isinstance(payload, dict):
            return payload
        return {"data": payload}
    except ValueError:
        return {"raw_text": response.text}
