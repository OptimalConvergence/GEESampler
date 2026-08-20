from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Literal

Geometry = Mapping[str, Any]
CollectionBuilder = Callable[["SampleRecord"], Any]
MaskBuilder = Callable[["SampleRecord"], Any]


def parse_datetime(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        seconds = float(value) / 1000.0 if abs(float(value)) > 10_000_000_000 else float(value)
        return datetime.fromtimestamp(seconds, tz=timezone.utc)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


@dataclass(frozen=True)
class SampleRecord:
    sample_id: str
    geometry: Geometry
    date: datetime | None = None
    properties: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not str(self.sample_id).strip():
            raise ValueError("sample_id cannot be empty")
        if self.geometry.get("type") not in {
            "Point",
            "MultiPoint",
            "Polygon",
            "MultiPolygon",
        }:
            raise ValueError(f"Unsupported geometry type: {self.geometry.get('type')}")


@dataclass(frozen=True)
class PatchGrid:
    size: int = 336
    scale: float = 10.0

    def __post_init__(self) -> None:
        if self.size <= 0:
            raise ValueError("patch size must be positive")
        if self.scale <= 0:
            raise ValueError("scale must be positive")


@dataclass(frozen=True)
class SceneSelection:
    mode: Literal["closest", "latest", "earliest", "all"] = "closest"
    start_offset_days: int = -30
    end_offset_days: int = 30
    max_scenes: int = 1

    def __post_init__(self) -> None:
        if self.start_offset_days > self.end_offset_days:
            raise ValueError("start_offset_days must not exceed end_offset_days")
        if self.max_scenes <= 0:
            raise ValueError("max_scenes must be positive")


@dataclass(frozen=True)
class AuthConfig:
    project: str
    service_account: str | None = None
    key_file: Path | None = None
    high_volume: bool = False


@dataclass(frozen=True)
class EECUMonitorConfig:
    enabled: bool = True
    required: bool = False
    poll_seconds: float = 30.0
    warning_eecu_hours: float | None = None
    hard_eecu_hours: float | None = None
    price_per_eecu_hour: float | None = None

    def __post_init__(self) -> None:
        if self.poll_seconds <= 0:
            raise ValueError("poll_seconds must be positive")
        for name in ("warning_eecu_hours", "hard_eecu_hours", "price_per_eecu_hour"):
            value = getattr(self, name)
            if value is not None and value < 0:
                raise ValueError(f"{name} must be non-negative")
        if (
            self.warning_eecu_hours is not None
            and self.hard_eecu_hours is not None
            and self.warning_eecu_hours > self.hard_eecu_hours
        ):
            raise ValueError("warning EECU limit cannot exceed hard EECU limit")


@dataclass(frozen=True)
class RunConfig:
    output_dir: Path
    workers: int = 8
    retries: int = 4
    retry_base_seconds: float = 1.0
    workload_prefix: str = "geesampler"
    eecu: EECUMonitorConfig = field(default_factory=EECUMonitorConfig)

    def __post_init__(self) -> None:
        if self.workers <= 0:
            raise ValueError("workers must be positive")
        if self.retries < 0:
            raise ValueError("retries must be non-negative")


@dataclass(frozen=True)
class TaskResult:
    task_id: str
    sample_id: str
    status: Literal["success", "failed", "skipped"]
    output_path: str | None
    bytes_downloaded: int
    elapsed_seconds: float
    attempts: int
    error: str | None = None
    scene_id: str | None = None
    scene_date: str | None = None
    target_date: str | None = None
    geometry: Geometry | None = None
    sample_properties: Mapping[str, Any] = field(default_factory=dict)
    clear_fraction: float | None = None
    timings: Mapping[str, float] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RunSummary:
    run_id: str
    workload_tag: str
    total: int
    succeeded: int
    failed: int
    skipped: int
    bytes_downloaded: int
    elapsed_seconds: float
    samples_per_second: float
    bandwidth_mib_per_second: float
    completed_eecu_seconds: float | None = None
    in_progress_eecu_seconds: float | None = None
    stopped_by_eecu_budget: bool = False
    catalog_metrics: Mapping[str, Any] = field(default_factory=dict)
    results: Sequence[TaskResult] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["results"] = [item.to_dict() for item in self.results]
        return result


DEFAULT_PATCH_GRID = PatchGrid()
DEFAULT_SCENE_SELECTION = SceneSelection()
