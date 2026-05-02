"""
Module: config.py
Description: Loads YAML configuration used by all project components.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "config.yaml"


def load_config(config_path: str | Path | None = None) -> dict[str, Any]:
    """
    Load the project configuration file.

    Parameters
    ----------
    config_path : str | Path | None
        Optional path to config.yaml. When omitted, the repository default is used.

    Returns
    -------
    dict[str, Any]
        Parsed YAML configuration.
    """
    path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")

    with path.open("r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    if not isinstance(config, dict):
        raise ValueError(f"Configuration file is empty or invalid: {path}")

    return config


def get_nested(config: dict[str, Any], *keys: str, default: Any = None) -> Any:
    """
    Safely fetch a nested config value.

    Parameters
    ----------
    config : dict[str, Any]
        Parsed configuration.
    *keys : str
        Path of nested keys.
    default : Any
        Fallback value when any key is missing.

    Returns
    -------
    Any
        Config value or default.
    """
    current: Any = config
    for key in keys:
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]
    return current
