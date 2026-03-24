"""Project settings and YAML configuration loading."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Environment-backed runtime settings."""

    model_config = SettingsConfigDict(env_prefix="GA_", extra="ignore")

    store_path: str = "storage/opportunities.duckdb"
    mt5_login: int | None = None
    mt5_password: str | None = None
    mt5_server: str | None = None
    mt5_path: str | None = None
    mt5_magic_number: int = 431000
    mt5_deviation: int = 20


def load_yaml_config(path: str | Path) -> dict[str, Any]:
    """Load a YAML configuration file into a dictionary."""

    payload = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Config at '{path}' must contain a mapping at the root.")
    return payload
