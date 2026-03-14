"""
Configuration loader and validator for the MLOps batch pipeline.
Loads YAML config, validates schema, and returns a typed Config dataclass.
"""

import yaml
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    """Immutable, typed pipeline configuration."""
    seed: int
    window: int
    version: str


def load_config(path: str) -> Config:
    """
    Load and validate pipeline configuration from a YAML file.

    Args:
        path: Path to the YAML configuration file.

    Returns:
        Validated Config dataclass instance.

    Raises:
        FileNotFoundError: If config file does not exist.
        ValueError: If required fields are missing or have invalid types.
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"Config file not found: {path}")
    except yaml.YAMLError as e:
        raise ValueError(f"Failed to parse YAML config: {e}")

    if not isinstance(raw, dict):
        raise ValueError("Config file must contain a YAML mapping (key-value pairs)")

    # Validate required fields
    required_fields = ["seed", "window", "version"]
    for field in required_fields:
        if field not in raw:
            raise ValueError(f"Missing required config field: '{field}'")

    # Validate types
    try:
        seed = int(raw["seed"])
    except (TypeError, ValueError):
        raise ValueError(
            f"Config field 'seed' must be integer, got {type(raw['seed']).__name__}"
        )

    try:
        window = int(raw["window"])
    except (TypeError, ValueError):
        raise ValueError(
            f"Config field 'window' must be integer, got {type(raw['window']).__name__}"
        )

    if window < 1:
        raise ValueError(f"Config field 'window' must be >= 1, got {window}")

    version = str(raw["version"]).strip()
    if not version:
        raise ValueError("Config field 'version' must be a non-empty string")

    return Config(seed=seed, window=window, version=version)
