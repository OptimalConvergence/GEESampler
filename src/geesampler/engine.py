from __future__ import annotations

import csv
import json
import logging
import os
import random
import re
import sqlite3
import threading
import time
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .grid import compute_grid
from .models import (
    DEFAULT_PATCH_GRID,
    DEFAULT_SCENE_SELECTION,
    CollectionBuilder,
    MaskBuilder,
    PatchGrid,
    RunConfig,
    RunSummary,
    SampleRecord,
    SceneSelection,
    TaskResult,
)
from .monitoring import CloudEECUReader, EECUMonitor

LOGGER = logging.getLogger(__name__)


def make_workload_tag(prefix: str, scenario: str, run_id: str) -> str:
    raw = re.sub(r"[^a-z0-9_-]+", "-", f"{prefix}-{scenario}-{run_id}".lower())
    tag = raw.strip("-_")[:63].rstrip("-_")
    if not tag:
        raise ValueError("workload tag is empty after sanitization")
    return tag


def _safe_name(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_.-]+", "_", value).strip("._") or "sample"


def _iso_from_millis(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return datetime.fromtimestamp(float(value) / 1000.0, tz=timezone.utc).isoformat()


class TaskLedger:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(path, check_same_thread=False)
        self._lock = threading.Lock()
        with self._db:
            self._db.execute(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    task_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    output_path TEXT,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    bytes_downloaded INTEGER NOT NULL DEFAULT 0,
                    elapsed_seconds REAL NOT NULL DEFAULT 0,
                    error TEXT,
                    updated_at TEXT NOT NULL
                )
                """
            )

    def status(self, task_id: str) -> str | None:
        with self._lock:
            row = self._db.execute(
                "SELECT status FROM tasks WHERE task_id = ?", (task_id,)
            ).fetchone()
        return row[0] if row else None

    def record(self, result: TaskResult) -> None:
        with self._lock, self._db:
            self._db.execute(
                """
                INSERT INTO tasks (
                    task_id, status, output_path, attempts, bytes_downloaded,
                    elapsed_seconds, error, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(task_id) DO UPDATE SET
                    status=excluded.status,
                    output_path=excluded.output_path,
                    attempts=excluded.attempts,
                    bytes_downloaded=excluded.bytes_downloaded,
                    elapsed_seconds=excluded.elapsed_seconds,
                    error=excluded.error,
                    updated_at=excluded.updated_at
                """,
                (
                    result.task_id,
                    result.status,
                    result.output_path,
                    result.attempts,
                    result.bytes_downloaded,
                    result.elapsed_seconds,
                    result.error,
                    datetime.now(timezone.utc).isoformat(),
                ),
            )

    def close(self) -> None:
        with self._lock:
            self._db.close()


class MetricsRecorder:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def append(self, result: TaskResult) -> None:
        event = result.to_dict()
        event["timestamp"] = datetime.now(timezone.utc).isoformat()
        with self._lock, self.path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(event, sort_keys=True) + "\n")


def _features(result: Any) -> list[Mapping[str, Any]]:
    if isinstance(result, Mapping):
        return list(result.get("features", []))
    return list(result)


class DownloadEngine:
    def __init__(
        self,
        project: str,
        config: RunConfig,
        *,
        ee_module: Any | None = None,
        eecu_reader_factory: Callable[[str, str], Any] | None = None,
    ):
        self.project = project
        self.config = config
        if ee_module is None:
            import ee

            ee_module = ee
        self.ee = ee_module
        self.eecu_reader_factory = eecu_reader_factory

    def download_patch_series(
        self,
        records: Iterable[SampleRecord],
        collection_builder: CollectionBuilder,
        *,
        bands: Sequence[str],
        grid: PatchGrid = DEFAULT_PATCH_GRID,
        selection: SceneSelection = DEFAULT_SCENE_SELECTION,
        mask_builder: MaskBuilder | None = None,
        scenario: str = "patches",
        run_id: str | None = None,
    ) -> RunSummary:
        samples = list(records)
        return self._run(
            samples,
            scenario,
            run_id,
            lambda sample, tag, ledger, run_dir: self._patch_worker(
                sample,
                tag,
                ledger,
                run_dir,
                collection_builder,
                tuple(bands),
                grid,
                selection,
                mask_builder,
            ),
        )

    def download_point_series(
        self,
        records: Iterable[SampleRecord],
        collection_builder: CollectionBuilder,
        *,
        bands: Sequence[str],
        scale: float = 10.0,
        scenario: str = "points",
        run_id: str | None = None,
    ) -> RunSummary:
        samples = list(records)
        return self._run(
            samples,
            scenario,
            run_id,
            lambda sample, tag, ledger, run_dir: [
                self._point_worker(
                    sample, tag, ledger, run_dir, collection_builder, tuple(bands), scale
                )
            ],
        )

    def _run(
        self,
        samples: Sequence[SampleRecord],
        scenario: str,
        run_id: str | None,
        worker: Callable[[SampleRecord, str, TaskLedger, Path], list[TaskResult]],
    ) -> RunSummary:
        run_id = (
            run_id
            or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]
        )
        workload_tag = make_workload_tag(self.config.workload_prefix, scenario, run_id)
        run_dir = self.config.output_dir / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        ledger = TaskLedger(run_dir / "ledger.sqlite")
        recorder = MetricsRecorder(run_dir / "metrics.jsonl")
        results: list[TaskResult] = []
        result_lock = threading.Lock()

        def completed_samples() -> int:
            with result_lock:
                return len({item.sample_id for item in results if item.status == "success"})

        reader = None
        if self.config.eecu.enabled:
            try:
                factory = self.eecu_reader_factory or CloudEECUReader
                reader = factory(self.project, workload_tag)
            except Exception as exc:
                if self.config.eecu.required:
                    ledger.close()
                    raise
                LOGGER.warning("EECU monitor unavailable at startup: %s", exc)
        monitor = EECUMonitor(
            self.config.eecu,
            reader,
            run_dir / "eecu.jsonl",
            completed_samples,
        )
        started = time.monotonic()
        monitor.start()
        stopped_by_budget = False
        pending = iter(samples)
        futures: dict[Future[list[TaskResult]], SampleRecord] = {}

        def submit_one(pool: ThreadPoolExecutor) -> bool:
            nonlocal stopped_by_budget
            if monitor.hard_limit_reached:
                stopped_by_budget = True
                return False
            try:
                sample = next(pending)
            except StopIteration:
                return False
            futures[pool.submit(worker, sample, workload_tag, ledger, run_dir)] = sample
            return True

        try:
            with ThreadPoolExecutor(max_workers=self.config.workers) as pool:
                for _ in range(min(self.config.workers, len(samples))):
                    submit_one(pool)
                while futures:
                    done, _ = wait(futures, return_when=FIRST_COMPLETED)
                    for future in done:
                        sample = futures.pop(future)
                        try:
                            batch = future.result()
                        except Exception as exc:  # noqa: BLE001 - isolate a failed worker
                            batch = [
                                TaskResult(
                                    task_id=f"{_safe_name(sample.sample_id)}-sample",
                                    sample_id=sample.sample_id,
                                    status="failed",
                                    output_path=None,
                                    bytes_downloaded=0,
                                    elapsed_seconds=0,
                                    attempts=1,
                                    error=str(exc),
                                )
                            ]
                        batch = [
                            replace(
                                item,
                                target_date=sample.date.isoformat() if sample.date else None,
                                geometry=dict(sample.geometry),
                                sample_properties=dict(sample.properties),
                            )
                            for item in batch
                        ]
                        with result_lock:
                            results.extend(batch)
                        for item in batch:
                            ledger.record(item)
                            recorder.append(item)
                        self._log_progress(results, len(samples), started, monitor)
                        submit_one(pool)
        finally:
            monitor.stop()
            ledger.close()

        if stopped_by_budget:
            for sample in pending:
                result = TaskResult(
                    task_id=f"{_safe_name(sample.sample_id)}-budget",
                    sample_id=sample.sample_id,
                    status="skipped",
                    output_path=None,
                    bytes_downloaded=0,
                    elapsed_seconds=0,
                    attempts=0,
                    error="EECU scheduling ceiling reached",
                    target_date=sample.date.isoformat() if sample.date else None,
                    geometry=dict(sample.geometry),
                    sample_properties=dict(sample.properties),
                )
                results.append(result)
                recorder.append(result)
        elapsed = time.monotonic() - started
        summary = self._summary(
            run_id, workload_tag, len(samples), results, elapsed, monitor, stopped_by_budget
        )
        (run_dir / "summary.json").write_text(
            json.dumps(summary.to_dict(), indent=2, sort_keys=True), encoding="utf-8"
        )
        self._write_manifest(run_dir / "manifest.csv", results)
        return summary

    def _patch_worker(
        self,
        sample: SampleRecord,
        workload_tag: str,
        ledger: TaskLedger,
        run_dir: Path,
        collection_builder: CollectionBuilder,
        bands: tuple[str, ...],
        grid_spec: PatchGrid,
        selection: SceneSelection,
        mask_builder: MaskBuilder | None,
    ) -> list[TaskResult]:
        started = time.monotonic()
        if sample.date is None:
            raise ValueError(f"Patch sample {sample.sample_id} has no target date")
        collection = collection_builder(sample)
        selected, metadata = self._select_scenes(collection, sample.date, selection, workload_tag)
        if not metadata:
            return [
                TaskResult(
                    task_id=f"{_safe_name(sample.sample_id)}-no-scene",
                    sample_id=sample.sample_id,
                    status="failed",
                    output_path=None,
                    bytes_downloaded=0,
                    elapsed_seconds=time.monotonic() - started,
                    attempts=1,
                    error="No qualifying scenes",
                )
            ]
        results: list[TaskResult] = []
        image_list = selected.toList(selection.max_scenes)
        for index, props in enumerate(metadata):
            scene_id = str(props.get("scene_id") or f"scene-{index:03d}")
            scene_date = _iso_from_millis(props.get("scene_time"))
            task_id = f"{_safe_name(sample.sample_id)}-{_safe_name(scene_id)}"
            output = (
                run_dir
                / "images"
                / _safe_name(sample.sample_id)
                / f"{index:03d}_{_safe_name(scene_id)}.tif"
            )
            mask_path = (
                run_dir
                / "masks"
                / _safe_name(sample.sample_id)
                / f"{index:03d}_{_safe_name(scene_id)}_mask.tif"
            )
            if ledger.status(task_id) == "success" and self._valid_file(output):
                results.append(
                    TaskResult(
                        task_id,
                        sample.sample_id,
                        "skipped",
                        str(output),
                        0,
                        0,
                        0,
                        scene_id=scene_id,
                        scene_date=scene_date,
                    )
                )
                continue
            image = self.ee.Image(image_list.get(index)).select(list(bands))
            request_bands = list(bands)
            has_mask = mask_builder is not None
            if has_mask:
                mask = self.ee.Image(mask_builder(sample)).rename("sample_mask").unmask(0).uint8()
                image = image.addBands(mask)
                request_bands.append("sample_mask")
            grid = compute_grid(sample.geometry, grid_spec)
            result = self._download_pixels(
                task_id,
                sample.sample_id,
                image,
                request_bands,
                grid.to_ee(),
                workload_tag,
                output,
                mask_path if has_mask else None,
                scene_id,
                scene_date,
            )
            results.append(result)
        return results

    def _select_scenes(
        self,
        collection: Any,
        target: datetime,
        selection: SceneSelection,
        workload_tag: str,
    ) -> tuple[Any, list[Mapping[str, Any]]]:
        start = target + timedelta(days=selection.start_offset_days)
        end = target + timedelta(days=selection.end_offset_days + 1)
        selected = collection.filterDate(start.isoformat(), end.isoformat())
        target_ms = int(target.timestamp() * 1000)
        if selection.mode == "closest":
            selected = selected.map(
                lambda image: image.set(
                    "_geesampler_delta",
                    self.ee.Number(image.get("system:time_start")).subtract(target_ms).abs(),
                )
            ).sort("_geesampler_delta")
        elif selection.mode == "latest":
            selected = selected.sort("system:time_start", False)
        else:
            selected = selected.sort("system:time_start")
        selected = selected.limit(selection.max_scenes)
        image_list = selected.toList(selection.max_scenes)
        metadata_fc = self.ee.FeatureCollection(
            image_list.map(
                lambda raw: self.ee.Feature(
                    None,
                    {
                        "scene_id": self.ee.Image(raw).get("system:index"),
                        "scene_time": self.ee.Image(raw).get("system:time_start"),
                    },
                )
            )
        )
        raw = self.ee.data.computeFeatures({"expression": metadata_fc, "workloadTag": workload_tag})
        metadata = [dict(item.get("properties", {})) for item in _features(raw)]
        return selected, metadata

    def _download_pixels(
        self,
        task_id: str,
        sample_id: str,
        image: Any,
        bands: list[str],
        grid: Mapping[str, Any],
        workload_tag: str,
        output: Path,
        mask_output: Path | None,
        scene_id: str,
        scene_date: str | None,
    ) -> TaskResult:
        output.parent.mkdir(parents=True, exist_ok=True)
        if mask_output:
            mask_output.parent.mkdir(parents=True, exist_ok=True)
        attempts = 0
        started = time.monotonic()
        error: Exception | None = None
        while attempts <= self.config.retries:
            attempts += 1
            temp = output.with_suffix(output.suffix + f".{uuid.uuid4().hex}.partial")
            mask_temp = (
                mask_output.with_suffix(mask_output.suffix + f".{uuid.uuid4().hex}.partial")
                if mask_output
                else None
            )
            try:
                payload = self.ee.data.computePixels(
                    {
                        "expression": image,
                        "fileFormat": "GEO_TIFF",
                        "bandIds": bands,
                        "grid": dict(grid),
                        "workloadTag": workload_tag,
                    }
                )
                temp.write_bytes(payload)
                if mask_output and mask_temp:
                    data_temp = output.with_suffix(
                        output.suffix + f".{uuid.uuid4().hex}.data.partial"
                    )
                    self._split_mask(temp, data_temp, mask_temp)
                    temp.unlink(missing_ok=True)
                    os.replace(data_temp, output)
                    os.replace(mask_temp, mask_output)
                else:
                    os.replace(temp, output)
                elapsed = time.monotonic() - started
                return TaskResult(
                    task_id,
                    sample_id,
                    "success",
                    str(output),
                    len(payload),
                    elapsed,
                    attempts,
                    scene_id=scene_id,
                    scene_date=scene_date,
                )
            except Exception as exc:  # noqa: BLE001 - remote errors are retried uniformly
                error = exc
                temp.unlink(missing_ok=True)
                if mask_temp:
                    mask_temp.unlink(missing_ok=True)
                if attempts <= self.config.retries:
                    delay = self.config.retry_base_seconds * (2 ** (attempts - 1))
                    time.sleep(delay + random.random() * min(1.0, delay / 4.0))
        return TaskResult(
            task_id,
            sample_id,
            "failed",
            None,
            0,
            time.monotonic() - started,
            attempts,
            error=str(error),
            scene_id=scene_id,
            scene_date=scene_date,
        )

    @staticmethod
    def _split_mask(combined: Path, image_output: Path, mask_output: Path) -> None:
        try:
            import rasterio
        except ImportError as exc:  # pragma: no cover
            raise ImportError("Polygon masks require the 'geo' extra (rasterio)") from exc
        with rasterio.open(combined) as source:
            if source.count < 2:
                raise ValueError("Combined image does not contain data plus a mask band")
            data_profile = source.profile.copy()
            data_profile.update(count=source.count - 1)
            with rasterio.open(image_output, "w", **data_profile) as destination:
                destination.write(source.read(indexes=list(range(1, source.count))))
            mask_profile = source.profile.copy()
            mask_profile.update(count=1, dtype="uint8", nodata=0)
            with rasterio.open(mask_output, "w", **mask_profile) as destination:
                destination.write((source.read(source.count) > 0).astype("uint8"), 1)

    def _point_worker(
        self,
        sample: SampleRecord,
        workload_tag: str,
        ledger: TaskLedger,
        run_dir: Path,
        collection_builder: CollectionBuilder,
        bands: tuple[str, ...],
        scale: float,
    ) -> TaskResult:
        task_id = f"{_safe_name(sample.sample_id)}-timeseries"
        output = run_dir / "timeseries" / f"{_safe_name(sample.sample_id)}.csv"
        if ledger.status(task_id) == "success" and self._valid_file(output):
            return TaskResult(task_id, sample.sample_id, "skipped", str(output), 0, 0, 0)
        output.parent.mkdir(parents=True, exist_ok=True)
        started = time.monotonic()
        attempts = 0
        error: Exception | None = None
        geometry = self.ee.Geometry(sample.geometry)
        collection = collection_builder(sample).select(list(bands))
        while attempts <= self.config.retries:
            attempts += 1
            try:
                images = collection.toList(collection.size())
                rows = images.map(
                    lambda raw: self.ee.Feature(
                        None,
                        self.ee.Image(raw)
                        .reduceRegion(
                            reducer=self.ee.Reducer.first(),
                            geometry=geometry,
                            scale=scale,
                        )
                        .combine(
                            self.ee.Dictionary(
                                {
                                    "scene_id": self.ee.Image(raw).get("system:index"),
                                    "scene_time": self.ee.Image(raw).get("system:time_start"),
                                }
                            )
                        ),
                    )
                )
                raw = self.ee.data.computeFeatures(
                    {
                        "expression": self.ee.FeatureCollection(rows),
                        "workloadTag": workload_tag,
                    }
                )
                features = _features(raw)
                temp = output.with_suffix(".csv.partial")
                properties = [dict(item.get("properties", {})) for item in features]
                columns = sorted({key for row in properties for key in row})
                with temp.open("w", newline="", encoding="utf-8") as stream:
                    writer = csv.DictWriter(stream, fieldnames=columns)
                    writer.writeheader()
                    writer.writerows(properties)
                os.replace(temp, output)
                return TaskResult(
                    task_id,
                    sample.sample_id,
                    "success",
                    str(output),
                    output.stat().st_size,
                    time.monotonic() - started,
                    attempts,
                )
            except Exception as exc:  # noqa: BLE001 - remote errors are retried uniformly
                error = exc
                if attempts <= self.config.retries:
                    time.sleep(self.config.retry_base_seconds * (2 ** (attempts - 1)))
        return TaskResult(
            task_id,
            sample.sample_id,
            "failed",
            None,
            0,
            time.monotonic() - started,
            attempts,
            error=str(error),
        )

    @staticmethod
    def _valid_file(path: Path) -> bool:
        return path.is_file() and path.stat().st_size > 0

    @staticmethod
    def _write_manifest(path: Path, results: Sequence[TaskResult]) -> None:
        columns = (
            list(asdict(results[0]).keys()) if results else list(TaskResult.__dataclass_fields__)
        )
        with path.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=columns)
            writer.writeheader()
            for item in results:
                row = item.to_dict()
                row["geometry"] = (
                    json.dumps(row["geometry"], sort_keys=True) if row["geometry"] else ""
                )
                row["sample_properties"] = json.dumps(
                    row["sample_properties"], sort_keys=True, default=str
                )
                writer.writerow(row)

    @staticmethod
    def _summary(
        run_id: str,
        workload_tag: str,
        total: int,
        results: Sequence[TaskResult],
        elapsed: float,
        monitor: EECUMonitor,
        stopped: bool,
    ) -> RunSummary:
        statuses: dict[str, set[str]] = {}
        for item in results:
            statuses.setdefault(item.sample_id, set()).add(item.status)
        succeeded = sum("success" in values for values in statuses.values())
        failed = sum("success" not in values and "failed" in values for values in statuses.values())
        skipped = sum(values == {"skipped"} for values in statuses.values())
        byte_count = sum(item.bytes_downloaded for item in results)
        latest = monitor.latest
        return RunSummary(
            run_id=run_id,
            workload_tag=workload_tag,
            total=total,
            succeeded=succeeded,
            failed=failed,
            skipped=skipped,
            bytes_downloaded=byte_count,
            elapsed_seconds=elapsed,
            samples_per_second=succeeded / elapsed if elapsed else 0.0,
            bandwidth_mib_per_second=byte_count / (1024**2) / elapsed if elapsed else 0.0,
            completed_eecu_seconds=latest.completed_seconds if latest else None,
            in_progress_eecu_seconds=latest.in_progress_seconds if latest else None,
            stopped_by_eecu_budget=stopped,
            results=tuple(results),
        )

    @staticmethod
    def _log_progress(
        results: Sequence[TaskResult],
        total_samples: int,
        started: float,
        monitor: EECUMonitor,
    ) -> None:
        elapsed = max(time.monotonic() - started, 1e-9)
        completed_ids = {item.sample_id for item in results}
        successful = len({item.sample_id for item in results if item.status == "success"})
        bytes_downloaded = sum(item.bytes_downloaded for item in results)
        completion_rate = len(completed_ids) / elapsed
        eta = (total_samples - len(completed_ids)) / completion_rate if completion_rate else None
        eecu = monitor.latest
        LOGGER.info(
            "progress=%d/%d success=%d elapsed=%.1fs eta=%s samples/s=%.3f "
            "bandwidth=%.3f MiB/s "
            "reported_eecu_completed=%s reported_eecu_in_progress=%s",
            len(completed_ids),
            total_samples,
            successful,
            elapsed,
            f"{eta:.1f}s" if eta is not None else "n/a",
            successful / elapsed,
            bytes_downloaded / (1024**2) / elapsed,
            f"{eecu.completed_seconds:.2f}" if eecu is not None else "unavailable",
            f"{eecu.in_progress_seconds:.2f}" if eecu is not None else "unavailable",
        )
