from __future__ import annotations

import base64
import json
import time
import uuid
from dataclasses import dataclass
from getpass import getpass
from typing import Any

import requests
from requests import Session


FARMENEUR_APP_URL = "https://app.farmeneur.com"


class InvalidCredentialsError(Exception):
    pass


@dataclass(frozen=True)
class FarmeneurSession:
    email: str
    farmeneur_token: str
    farmerzone_token: str
    factulotes_token: str
    farmer_id: str
    device_id: str
    session_id: str
    raw_login_response: dict[str, Any]


def prompt_farmeneur_credentials(default_email: str | None = None) -> tuple[str, str]:
    email_prompt = "Farmeneur email"
    if default_email:
        email_prompt += f" [{default_email}]"
    email_prompt += ": "

    entered_email = input(email_prompt).strip()
    email = entered_email or (default_email or "")
    if not email:
        raise ValueError("Farmeneur email is required")

    password = getpass("Farmeneur password: ")
    if not password:
        raise ValueError("Farmeneur password is required")

    return email, password


def authenticate_farmeneur(
    email: str,
    password: str,
    timeout_seconds: int,
    session: Session | None = None,
) -> FarmeneurSession:
    http = session or requests.Session()
    device_id = str(uuid.uuid4())
    session_id = _generate_session_id()

    common_headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en",
        "Origin": FARMENEUR_APP_URL,
        "Referer": f"{FARMENEUR_APP_URL}/en/auth/login",
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/26.5 Safari/605.1.15"
        ),
        "X-Origin-CF": FARMENEUR_APP_URL,
        "X-Version": "0.0.1",
        "X-Device-CF": device_id,
        "X-SessionId": session_id,
    }

    login_response = http.post(
        f"{FARMENEUR_APP_URL}/services/farmeneur/v1/auth/login",
        headers={
            **common_headers,
            "Content-Type": "application/json",
            "showDefaultSpinner": "false",
        },
        json={"email": email, "password": password},
        timeout=timeout_seconds,
    )
    if login_response.status_code == 401:
        raise InvalidCredentialsError("Credenciales de Farmeneur incorrectas")
    login_response.raise_for_status()
    login_payload = login_response.json()

    farmeneur_token = _extract_nested_string(login_payload, "data", "token")

    service_headers = {
        **common_headers,
        "Authorization": farmeneur_token,
        "Referer": f"{FARMENEUR_APP_URL}/en/crowdfarming/factulotes",
    }

    farmerzone_response = http.get(
        f"{FARMENEUR_APP_URL}/services/farmeneur/v1/farmerzone/token",
        headers=service_headers,
        timeout=timeout_seconds,
    )
    farmerzone_response.raise_for_status()
    farmerzone_token = _extract_nested_string(farmerzone_response.json(), "data", "token")

    farmer_id = _extract_farmer_id_from_farmerzone_token(farmerzone_token)

    factulotes_exchange_response = http.get(
        f"{FARMENEUR_APP_URL}/services/farmeneur/v1/factulotes/token",
        headers=service_headers,
        timeout=timeout_seconds,
    )
    factulotes_exchange_response.raise_for_status()
    factulotes_exchange_token = _extract_nested_string(
        factulotes_exchange_response.json(),
        "data",
        "token",
    )

    factulotes_exchange = http.post(
        "https://factulotes-api.crowdfarming.com/auth/exchange",
        headers={
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
            "Origin": FARMENEUR_APP_URL,
            "Referer": "https://factulotes.es/",
            "User-Agent": common_headers["User-Agent"],
            "X-Origin-CF": FARMENEUR_APP_URL,
        },
        json={"exchangeToken": factulotes_exchange_token},
        timeout=timeout_seconds,
    )
    factulotes_exchange.raise_for_status()
    factulotes_token = _extract_top_level_string(factulotes_exchange.json(), "accessToken")

    return FarmeneurSession(
        email=email,
        farmeneur_token=farmeneur_token,
        farmerzone_token=farmerzone_token,
        factulotes_token=factulotes_token,
        farmer_id=farmer_id,
        device_id=device_id,
        session_id=session_id,
        raw_login_response=login_payload if isinstance(login_payload, dict) else {"data": login_payload},
    )


def decode_jwt_payload(token: str) -> dict[str, Any]:
    parts = token.split(".")
    if len(parts) < 2:
        raise ValueError("Invalid JWT format")
    encoded_payload = parts[1]
    encoded_payload += "=" * (-len(encoded_payload) % 4)
    decoded = base64.urlsafe_b64decode(encoded_payload.encode("ascii"))
    payload = json.loads(decoded.decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Invalid JWT payload")
    return payload


def _generate_session_id() -> str:
    raw = f"{int(time.time() * 1000)}_$_Mac_$_Safari_$_26.5_$_null"
    return base64.b64encode(raw.encode("utf-8")).decode("ascii")


def _extract_farmer_id_from_farmerzone_token(token: str) -> str:
    payload = decode_jwt_payload(token)
    user = payload.get("user") if isinstance(payload, dict) else None
    farmer_id = user.get("_profile") if isinstance(user, dict) else None
    if not isinstance(farmer_id, str) or not farmer_id.strip():
        raise ValueError("Farmerzone token does not contain a usable farmer_id")
    return farmer_id


def _extract_nested_string(payload: dict[str, Any], *keys: str) -> str:
    current: Any = payload
    for key in keys:
        if not isinstance(current, dict):
            raise ValueError(f"Expected dict while resolving {'.'.join(keys)}")
        current = current.get(key)
    if not isinstance(current, str) or not current.strip():
        raise ValueError(f"Missing string value at {'.'.join(keys)}")
    return current


def _extract_top_level_string(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Missing string value at {key}")
    return value
