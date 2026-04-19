from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from env_file import load_env_file


@dataclass(frozen=True)
class Settings:
    crowdfarming_base_url: str
    factulotes_base_url: str
    farmeneur_email: str | None
    crowdfarming_token: str | None
    factulotes_token: str | None
    farmer_id: str | None
    tmp_dir: Path
    timeout_seconds: int
    max_retries: int


def _get_env(name: str, default: str | None = None, required: bool = False) -> str:
    value = os.getenv(name, default)
    if required and not value:
        raise ValueError(f"Missing required environment variable: {name}")
    if value is None:
        raise ValueError(f"Missing environment variable: {name}")
    return value


def load_settings(env_file: str | Path = ".env") -> Settings:
    load_env_file(Path(env_file))
    tmp_dir = Path(_get_env("TMP_DIR", "./tmp/payouts")).expanduser()
    timeout_seconds = int(_get_env("TIMEOUT_SECONDS", "60"))
    max_retries = int(_get_env("MAX_RETRIES", "3"))

    if timeout_seconds <= 0:
        raise ValueError("TIMEOUT_SECONDS must be greater than 0")
    if max_retries < 0:
        raise ValueError("MAX_RETRIES must be 0 or greater")

    return Settings(
        crowdfarming_base_url=_get_env(
            "CROWDFARMING_BASE_URL",
            "https://farmer.crowdfarming.com",
            required=True,
        ).rstrip("/"),
        factulotes_base_url=_get_env(
            "FACTULOTES_BASE_URL",
            "https://factulotes-api.crowdfarming.com",
            required=True,
        ).rstrip("/"),
        farmeneur_email=_get_env("FARMENEUR_EMAIL", default=None, required=False),
        crowdfarming_token=_get_env("CROWDFARMING_TOKEN", default=None, required=False),
        factulotes_token=_get_env("FACTULOTES_TOKEN", default=None, required=False),
        farmer_id=_get_env("FARMER_ID", default=None, required=False),
        tmp_dir=tmp_dir,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
    )
