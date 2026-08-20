from __future__ import annotations

import importlib
import os
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .models import AuthConfig, EECUMonitorConfig, PatchGrid, RunConfig, SceneSelection
from .resolver import CatalogResolverConfig

_ENV = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-(.*?))?\}")


def _expand(value: Any) -> Any:
    if isinstance(value, str):

        def replace(match: re.Match[str]) -> str:
            name, default = match.group(1), match.group(2)
            if name in os.environ:
                return os.environ[name]
            if default is not None:
                return default
            raise ValueError(f"Required environment variable is not set: {name}")

        return _ENV.sub(replace, value)
    if isinstance(value, list):
        return [_expand(item) for item in value]
    if isinstance(value, dict):
        return {key: _expand(item) for key, item in value.items()}
    return value


def load_callable(path: str) -> Callable[..., Any]:
    module_name, separator, attribute = path.partition(":")
    if not separator or not module_name or not attribute:
        raise ValueError("Callable paths must use 'module:function' syntax")
    value = getattr(importlib.import_module(module_name), attribute)
    if not callable(value):
        raise TypeError(f"Configured object is not callable: {path}")
    return value


@dataclass(frozen=True)
class CatalogSettings:
    path: Path
    resolver: CatalogResolverConfig


@dataclass(frozen=True)
class SamplerConfig:
    auth: AuthConfig
    run: RunConfig
    proxy_url: str | None
    raw: Mapping[str, Any]
    catalog: CatalogSettings | None = None

    @classmethod
    def from_yaml(cls, path: str | Path) -> SamplerConfig:
        payload = _expand(yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {})
        auth_data = payload.get("auth", {})
        if not auth_data.get("project"):
            raise ValueError("auth.project is required")
        auth = AuthConfig(
            project=str(auth_data["project"]),
            service_account=auth_data.get("service_account") or None,
            key_file=Path(auth_data["key_file"]).expanduser()
            if auth_data.get("key_file")
            else None,
            high_volume=bool(auth_data.get("high_volume", False)),
        )
        run_data = payload.get("run", {})
        monitor_data = run_data.get("monitoring", {})
        eecu = EECUMonitorConfig(
            enabled=bool(monitor_data.get("enabled", True)),
            required=bool(monitor_data.get("required", False)),
            poll_seconds=float(monitor_data.get("poll_seconds", 30)),
            warning_eecu_hours=monitor_data.get("warning_eecu_hours"),
            hard_eecu_hours=monitor_data.get("hard_eecu_hours"),
            price_per_eecu_hour=monitor_data.get("price_per_eecu_hour"),
        )
        output = Path(run_data.get("output_dir", "./geesampler-output")).expanduser()
        run = RunConfig(
            output_dir=output,
            workers=int(run_data.get("workers", 8)),
            retries=int(run_data.get("retries", 4)),
            retry_base_seconds=float(run_data.get("retry_base_seconds", 1)),
            workload_prefix=str(run_data.get("workload_prefix", "geesampler")),
            eecu=eecu,
        )
        catalog_data = payload.get("catalog", {})
        catalog = None
        if bool(catalog_data.get("enabled", False)):
            cloud_data = catalog_data.get("cloud", {})
            catalog = CatalogSettings(
                path=Path(
                    catalog_data.get("path", "./geesampler-output/catalog/sentinel2.sqlite")
                ).expanduser(),
                resolver=CatalogResolverConfig(
                    mode=str(catalog_data.get("mode", "read_through")),
                    metadata_workers=int(catalog_data.get("metadata_workers", 2)),
                    metadata_retries=int(catalog_data.get("metadata_retries", 4)),
                    retry_base_seconds=float(catalog_data.get("retry_base_seconds", 1)),
                    query_window_days=int(catalog_data.get("query_window_days", 366)),
                    max_tiles_per_query=int(catalog_data.get("max_tiles_per_query", 8)),
                    recent_horizon_days=int(catalog_data.get("recent_horizon_days", 30)),
                    recent_refresh_hours=int(catalog_data.get("recent_refresh_hours", 24)),
                    metadata_cloud_max=float(catalog_data.get("metadata_cloud_max", 20)),
                    cloud_mode=str(cloud_data.get("mode", "hybrid_inline")),
                    qa_band=str(cloud_data.get("band", "cs_cdf")),
                    qa_threshold=float(cloud_data.get("threshold", 0.60)),
                    min_clear_fraction=float(cloud_data.get("min_clear_fraction", 0.80)),
                    group_downloads=bool(catalog_data.get("group_downloads", True)),
                ),
            )
        return cls(auth, run, payload.get("proxy_url"), payload, catalog)


def patch_settings(payload: Mapping[str, Any]) -> tuple[PatchGrid, SceneSelection]:
    data = payload.get("download", {})
    selection = data.get("selection", {})
    return (
        PatchGrid(size=int(data.get("patch_size", 336)), scale=float(data.get("scale", 10))),
        SceneSelection(
            mode=selection.get("mode", "closest"),
            start_offset_days=int(selection.get("start_offset_days", -30)),
            end_offset_days=int(selection.get("end_offset_days", 30)),
            max_scenes=int(selection.get("max_scenes", 1)),
        ),
    )
