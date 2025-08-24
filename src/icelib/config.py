from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional

import yaml


class ConfigError(RuntimeError):
    pass


@dataclass
class FSConfig:
    endpoint_url: Optional[str] = None
    region: Optional[str] = None
    profile: Optional[str] = None


@dataclass
class SampleEngine:
    type: str = "local"


@dataclass
class Catalog:
    name: str
    type: str
    uri: Optional[str] = None
    warehouse: Optional[str] = None
    fs: FSConfig = field(default_factory=FSConfig)
    sample_engine: SampleEngine = field(default_factory=SampleEngine)
    overrides: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Config:
    version: int = 1
    default_catalog: Optional[str] = None
    catalogs: Dict[str, Catalog] = field(default_factory=dict)
    source_path: Optional[Path] = None

    def to_json(self) -> str:
        def _default(o: Any):
            if isinstance(o, Path):
                return str(o)
            if hasattr(o, "__dict__"):
                return o.__dict__
            return str(o)

        return json.dumps(self, default=_default, indent=2, sort_keys=True)


def _expand_env(value: Any) -> Any:
    if isinstance(value, str):
        # Expand ${VAR} style
        return os.path.expandvars(value)
    if isinstance(value, dict):
        return {k: _expand_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand_env(v) for v in value]
    return value


def _as_catalog(name: str, raw: Dict[str, Any]) -> Catalog:
    fs_raw = raw.get("fs", {}) or {}
    cat = Catalog(
        name=name,
        type=str(raw.get("type", "")).strip(),
        uri=raw.get("uri"),
        warehouse=raw.get("warehouse"),
        fs=FSConfig(
            endpoint_url=fs_raw.get("endpoint_url"),
            region=fs_raw.get("region"),
            profile=fs_raw.get("profile"),
        ),
        sample_engine=SampleEngine(**(raw.get("sample_engine") or {"type": "local"})),
        overrides=_expand_env(raw.get("overrides") or {}),
    )
    return cat


def resolve_config_path() -> Path:
    # Highest priority: explicit override
    override = os.environ.get("ICECTL_CONFIG")
    if override:
        p = Path(override).expanduser()
        if p.is_file():
            return p
        raise ConfigError(f"ICECTL_CONFIG path not found: {p}")

    # XDG base dirs
    xdg_home = Path(os.environ.get("XDG_CONFIG_HOME", "~/.config")).expanduser()
    candidates = [xdg_home / "icectl" / "config.yaml"]

    xdg_dirs = os.environ.get("XDG_CONFIG_DIRS", "/etc/xdg")
    for d in xdg_dirs.split(":"):
        candidates.append(Path(d) / "icectl" / "config.yaml")

    for c in candidates:
        if c.is_file():
            return c

    raise ConfigError(
        "No config file found. Set ICECTL_CONFIG or create ~/.config/icectl/config.yaml"
    )


def load_config(path: Optional[Path] = None) -> Config:
    cfg_path = path or resolve_config_path()
    try:
        data = yaml.safe_load(cfg_path.read_text()) or {}
    except FileNotFoundError as e:
        raise ConfigError(str(e)) from e

    data = _expand_env(data)
    catalogs_raw = data.get("catalogs") or {}
    catalogs: Dict[str, Catalog] = {
        name: _as_catalog(name, raw or {}) for name, raw in catalogs_raw.items()
    }

    cfg = Config(
        version=int(data.get("version", 1)),
        default_catalog=data.get("default_catalog"),
        catalogs=catalogs,
        source_path=cfg_path,
    )
    return cfg

