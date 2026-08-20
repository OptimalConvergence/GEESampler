from __future__ import annotations

import csv
import json
import uuid
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any

from .auth import initialize_earth_engine
from .config import SamplerConfig
from .engine import DownloadEngine
from .models import DEFAULT_SCENE_SELECTION, PatchGrid, RunSummary, SampleRecord, SceneSelection
from .monitoring import CloudEECUReader
from .visualize import plot_benchmark


@dataclass(frozen=True)
class BenchmarkCase:
    name: str
    patch_size: int
    workers: int
    high_volume: bool


DEFAULT_CASES = (
    BenchmarkCase("standard-336-w8", 336, 8, False),
    BenchmarkCase("highvolume-336-w8", 336, 8, True),
    BenchmarkCase("highvolume-128-w8", 128, 8, True),
    BenchmarkCase("highvolume-256-w8", 256, 8, True),
    BenchmarkCase("highvolume-512-w8", 512, 8, True),
    BenchmarkCase("highvolume-336-w4", 336, 4, True),
    BenchmarkCase("highvolume-336-w16", 336, 16, True),
)


def benchmark_patch_downloads(
    config: SamplerConfig,
    records: Iterable[SampleRecord],
    collection_builder_factory: Callable[[PatchGrid], Callable[[SampleRecord], Any]],
    *,
    bands: Sequence[str],
    output_dir: str | Path,
    selection: SceneSelection = DEFAULT_SCENE_SELECTION,
    cases: Sequence[BenchmarkCase] = DEFAULT_CASES,
    repetitions: int = 2,
    sample_count: int = 12,
    benchmark_id: str | None = None,
) -> Path:
    samples = list(records)[:sample_count]
    if not samples:
        raise ValueError("No benchmark samples")
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    benchmark_id = (
        benchmark_id or datetime.now(timezone.utc).strftime("%H%M%S") + "-" + uuid.uuid4().hex[:4]
    )
    raw_rows: list[dict[str, Any]] = []
    for case in cases:
        grid = PatchGrid(case.patch_size, 10)
        collection_builder = collection_builder_factory(grid)
        auth = replace(config.auth, high_volume=case.high_volume)
        ee = initialize_earth_engine(auth)
        run = replace(
            config.run,
            output_dir=output_dir / "runs" / case.name,
            workers=case.workers,
        )
        for repetition in range(repetitions):
            summary = DownloadEngine(auth.project, run, ee_module=ee).download_patch_series(
                samples,
                collection_builder,
                bands=bands,
                grid=grid,
                selection=selection,
                scenario="benchmark",
                run_id=f"{benchmark_id}-r{repetition + 1}-{case.name}",
            )
            raw_rows.append(_row(case, repetition + 1, summary))
    raw_path = output_dir / "benchmark_runs.csv"
    _write_rows(raw_path, raw_rows)
    aggregate_path = output_dir / "benchmark_summary.csv"
    _write_rows(aggregate_path, _aggregate(raw_rows))
    plot_benchmark(aggregate_path, output_dir / "benchmark_comparison.png")
    return aggregate_path


def _row(case: BenchmarkCase, repetition: int, summary: RunSummary) -> dict[str, Any]:
    return {
        "case": case.name,
        "repetition": repetition,
        "run_id": summary.run_id,
        "workload_tag": summary.workload_tag,
        "patch_size": case.patch_size,
        "workers": case.workers,
        "endpoint": "highvolume" if case.high_volume else "standard",
        "successful": summary.succeeded,
        "failed": summary.failed,
        "elapsed_seconds": summary.elapsed_seconds,
        "bandwidth_mib_per_second": summary.bandwidth_mib_per_second,
        "samples_per_second": summary.samples_per_second,
        "completed_eecu_seconds": (
            summary.completed_eecu_seconds if summary.completed_eecu_seconds is not None else ""
        ),
        "eecu_per_success": (
            summary.completed_eecu_seconds / summary.succeeded
            if summary.completed_eecu_seconds is not None and summary.succeeded
            else ""
        ),
    }


def refresh_benchmark_eecu(raw_path: str | Path, project: str) -> Path:
    """Refresh delayed completed-EECU values and regenerate aggregate outputs."""
    raw_path = Path(raw_path)
    with raw_path.open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    if not rows:
        raise ValueError("Benchmark CSV is empty")
    for row in rows:
        if not row.get("workload_tag"):
            _restore_run_identity(raw_path.parent, row)
        snapshot = CloudEECUReader(project, row["workload_tag"]).read()
        completed = snapshot.completed_seconds
        row["completed_eecu_seconds"] = completed
        successful = int(row["successful"])
        row["eecu_per_success"] = completed / successful if successful else ""
    _write_rows(raw_path, rows)
    aggregate_path = raw_path.parent / "benchmark_summary.csv"
    _write_rows(aggregate_path, _aggregate(rows))
    plot_benchmark(aggregate_path, raw_path.parent / "benchmark_comparison.png")
    return aggregate_path


def _restore_run_identity(output_dir: Path, row: dict[str, Any]) -> None:
    case = row["case"]
    repetition = row["repetition"]
    suffix = f"-r{repetition}-{case}"
    candidates = []
    for path in (output_dir / "runs" / case).glob("*/summary.json"):
        payload = json.loads(path.read_text(encoding="utf-8"))
        if str(payload.get("run_id", "")).endswith(suffix):
            candidates.append((path.stat().st_mtime, payload))
    if not candidates:
        raise FileNotFoundError(f"No summary matches benchmark row {case} repetition {repetition}")
    payload = max(candidates, key=lambda item: item[0])[1]
    row["run_id"] = payload["run_id"]
    row["workload_tag"] = payload["workload_tag"]


def _aggregate(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    result = []
    for case in dict.fromkeys(row["case"] for row in rows):
        group = [row for row in rows if row["case"] == case]
        first = group[0]
        eecu = [float(row["eecu_per_success"]) for row in group if row["eecu_per_success"] != ""]
        result.append(
            {
                "case": case,
                "patch_size": first["patch_size"],
                "workers": first["workers"],
                "endpoint": first["endpoint"],
                "successful": sum(int(row["successful"]) for row in group),
                "failed": sum(int(row["failed"]) for row in group),
                "bandwidth_mib_per_second": sum(
                    float(row["bandwidth_mib_per_second"]) for row in group
                )
                / len(group),
                "median_bandwidth_mib_per_second": median(
                    float(row["bandwidth_mib_per_second"]) for row in group
                ),
                "samples_per_second": sum(float(row["samples_per_second"]) for row in group)
                / len(group),
                "eecu_per_success": sum(eecu) / len(eecu) if eecu else "nan",
            }
        )
    return result


def _write_rows(path: Path, rows: Sequence[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
