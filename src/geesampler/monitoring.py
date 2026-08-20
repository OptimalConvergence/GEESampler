from __future__ import annotations

import json
import logging
import threading
from collections.abc import Callable, Iterable
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .models import EECUMonitorConfig

LOGGER = logging.getLogger(__name__)
COMPLETED_METRIC = "earthengine.googleapis.com/project/cpu/usage_time"
IN_PROGRESS_METRIC = "earthengine.googleapis.com/project/cpu/in_progress_usage_time"


@dataclass(frozen=True)
class EECUSnapshot:
    timestamp: str
    completed_seconds: float
    in_progress_seconds: float
    available: bool = True
    error: str | None = None

    @property
    def guard_seconds(self) -> float:
        """Best available usage for guarding without double-counting both metrics."""
        return max(self.completed_seconds, self.in_progress_seconds)


def _point_value(point: Any) -> float:
    value = point.value
    for name in ("double_value", "int64_value"):
        item = getattr(value, name, None)
        if item is not None:
            return float(item)
    return 0.0


class CloudEECUReader:
    """Read workload-tagged DELTA metrics from Cloud Monitoring."""

    def __init__(self, project: str, workload_tag: str, lookback_minutes: int = 30):
        try:
            from google.cloud import monitoring_v3
        except ImportError as exc:
            raise ImportError("EECU monitoring requires the 'monitoring' extra") from exc
        self.project = project
        self.workload_tag = workload_tag
        self.lookback_minutes = lookback_minutes
        self._monitoring_v3 = monitoring_v3
        self._client = monitoring_v3.MetricServiceClient()

    def _sum(self, metric_type: str) -> float:
        now = datetime.now(timezone.utc)
        interval = self._monitoring_v3.TimeInterval(
            {"end_time": now, "start_time": now - timedelta(minutes=self.lookback_minutes)}
        )
        metric_filter = (
            f'metric.type = "{metric_type}" AND metric.labels.workload_tag = "{self.workload_tag}"'
        )
        series = self._client.list_time_series(
            request={
                "name": f"projects/{self.project}",
                "filter": metric_filter,
                "interval": interval,
                "view": self._monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
            }
        )
        return sum(_point_value(point) for item in series for point in item.points)

    def read(self) -> EECUSnapshot:
        return EECUSnapshot(
            timestamp=datetime.now(timezone.utc).isoformat(),
            completed_seconds=self._sum(COMPLETED_METRIC),
            in_progress_seconds=self._sum(IN_PROGRESS_METRIC),
        )


class EECUMonitor:
    def __init__(
        self,
        config: EECUMonitorConfig,
        reader: CloudEECUReader | None,
        metrics_path: Path,
        completed_samples: Callable[[], int],
    ):
        self.config = config
        self.reader = reader
        self.metrics_path = metrics_path
        self.completed_samples = completed_samples
        self.latest: EECUSnapshot | None = None
        self.unavailable_error: str | None = None
        self.warning_reached = False
        self.hard_limit_reached = False
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._write_lock = threading.Lock()

    def start(self) -> None:
        if not self.config.enabled:
            return
        if self.reader is None:
            self.unavailable_error = "Cloud Monitoring reader is unavailable"
            if self.config.required:
                raise RuntimeError(self.unavailable_error)
            LOGGER.warning("EECU monitoring unavailable; continuing without an EECU guard")
            return
        self._thread = threading.Thread(target=self._loop, name="eecu-monitor", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=max(2.0, self.config.poll_seconds + 1.0))

    def poll_once(self) -> EECUSnapshot | None:
        if self.reader is None:
            return None
        try:
            snapshot = self.reader.read()
            self.latest = snapshot
            hours = snapshot.guard_seconds / 3600.0
            if (
                self.config.warning_eecu_hours is not None
                and hours >= self.config.warning_eecu_hours
            ):
                if not self.warning_reached:
                    LOGGER.warning("EECU warning threshold reached: %.4f EECU-hours", hours)
                self.warning_reached = True
            if self.config.hard_eecu_hours is not None and hours >= self.config.hard_eecu_hours:
                if not self.hard_limit_reached:
                    LOGGER.warning("EECU scheduling ceiling reached: %.4f EECU-hours", hours)
                self.hard_limit_reached = True
            self._append(snapshot)
            return snapshot
        except Exception as exc:  # noqa: BLE001 - optional telemetry must not crash downloads
            self.unavailable_error = str(exc)
            if self.config.required:
                self.hard_limit_reached = True
            LOGGER.warning("Unable to read EECU metrics: %s", exc)
            return None

    def _append(self, snapshot: EECUSnapshot) -> None:
        completed = self.completed_samples()
        payload = asdict(snapshot)
        payload["completed_samples"] = completed
        payload["eecu_per_success"] = snapshot.completed_seconds / completed if completed else None
        payload["projected_cost"] = (
            snapshot.completed_seconds / 3600.0 * self.config.price_per_eecu_hour
            if self.config.price_per_eecu_hour is not None
            else None
        )
        self.metrics_path.parent.mkdir(parents=True, exist_ok=True)
        with self._write_lock, self.metrics_path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(payload, sort_keys=True) + "\n")

    def _loop(self) -> None:
        while not self._stop.is_set():
            self.poll_once()
            self._stop.wait(self.config.poll_seconds)


class SequenceEECUReader:
    """Deterministic reader used by tests and offline demonstrations."""

    def __init__(self, snapshots: Iterable[EECUSnapshot]):
        self._snapshots = iter(snapshots)
        self._last: EECUSnapshot | None = None

    def read(self) -> EECUSnapshot:
        try:
            self._last = next(self._snapshots)
        except StopIteration:
            if self._last is None:
                raise RuntimeError("No EECU snapshots configured")
        return self._last
