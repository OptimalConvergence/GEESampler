from __future__ import annotations

import csv
import json
import sqlite3
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any

from .auth import initialize_earth_engine
from .catalog import S2SceneCatalog
from .config import SamplerConfig
from .engine import DownloadEngine, make_workload_tag
from .models import DEFAULT_SCENE_SELECTION, PatchGrid, RunSummary, SampleRecord, SceneSelection
from .monitoring import CloudEECUReader
from .resolver import S2CatalogResolver
from .visualize import plot_benchmark


@dataclass(frozen=True)
class BenchmarkCase:
    name: str
    patch_size: int
    workers: int
    high_volume: bool
    grouped: bool = True
    metadata_workers: int | None = None
    cloud_mode: str | None = None


DEFAULT_CASES = (
    BenchmarkCase("standard-336-w8", 336, 8, False),
    BenchmarkCase("highvolume-336-w8", 336, 8, True),
    BenchmarkCase("highvolume-128-w8", 128, 8, True),
    BenchmarkCase("highvolume-256-w8", 256, 8, True),
    BenchmarkCase("highvolume-512-w8", 512, 8, True),
    BenchmarkCase("highvolume-336-w4", 336, 4, True),
    BenchmarkCase("highvolume-336-w16", 336, 16, True),
    BenchmarkCase("highvolume-336-w24", 336, 24, True),
    BenchmarkCase("highvolume-336-w32", 336, 32, True),
    BenchmarkCase("highvolume-336-w16-random", 336, 16, True, False),
    BenchmarkCase("highvolume-336-w16-metadata-only", 336, 16, True, True, 2, "metadata_only"),
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
    repetitions: int = 3,
    sample_count: int = 128,
    benchmark_id: str | None = None,
) -> Path:
    samples = list(records)[:sample_count]
    if not samples:
        raise ValueError("No benchmark samples")
    if not cases:
        raise ValueError("No benchmark cases")
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    benchmark_id = (
        benchmark_id or datetime.now(timezone.utc).strftime("%H%M%S") + "-" + uuid.uuid4().hex[:4]
    )
    base_catalog_path = None
    if config.catalog is not None:
        base_catalog_path = output_dir / "catalogs" / f"{benchmark_id}-base.sqlite"
        metadata_ee = initialize_earth_engine(replace(config.auth, high_volume=False))
        prefill = S2CatalogResolver(
            S2SceneCatalog(base_catalog_path),
            ee_module=metadata_ee,
            config=replace(
                config.catalog.resolver,
                cloud_mode="metadata_only",
                group_downloads=True,
            ),
        )
        prefill_tag = make_workload_tag("geesampler", "benchmark-prefill", benchmark_id)
        prefill.prepare(
            samples,
            grid=PatchGrid(max(case.patch_size for case in cases), 10),
            selection=selection,
            workload_tag=prefill_tag,
        )
        prefill_payload = asdict(prefill.stats())
        prefill_payload["catalog_hit_rate"] = prefill.stats().catalog_hit_rate
        prefill_payload["workload_tag"] = prefill_tag
        (output_dir / "catalog_prefill.json").write_text(
            json.dumps(prefill_payload, indent=2, sort_keys=True), encoding="utf-8"
        )
    raw_rows: list[dict[str, Any]] = []
    for case in cases:
        grid = PatchGrid(case.patch_size, 10)
        collection_builder = collection_builder_factory(grid)
        auth = replace(config.auth, high_volume=case.high_volume)
        ee = initialize_earth_engine(auth)
        effective_case = replace(
            case,
            metadata_workers=case.metadata_workers
            if case.metadata_workers is not None
            else config.catalog.resolver.metadata_workers
            if config.catalog is not None
            else None,
            cloud_mode=case.cloud_mode
            or (config.catalog.resolver.cloud_mode if config.catalog is not None else None),
        )
        run = replace(
            config.run,
            output_dir=output_dir / "runs" / case.name,
            workers=case.workers,
        )
        for repetition in range(repetitions):
            resolver = None
            if config.catalog is not None and base_catalog_path is not None:
                case_catalog_path = (
                    output_dir / "catalogs" / f"{benchmark_id}-{case.name}-r{repetition + 1}.sqlite"
                )
                _copy_catalog(base_catalog_path, case_catalog_path)
                resolver = S2CatalogResolver(
                    S2SceneCatalog(case_catalog_path),
                    ee_module=ee,
                    config=replace(
                        config.catalog.resolver,
                        group_downloads=effective_case.grouped,
                        metadata_workers=effective_case.metadata_workers,
                        cloud_mode=effective_case.cloud_mode,
                    ),
                )
            summary = DownloadEngine(auth.project, run, ee_module=ee).download_patch_series(
                samples,
                collection_builder,
                bands=bands,
                grid=grid,
                selection=selection,
                scene_resolver=resolver,
                scenario="benchmark",
                run_id=f"{benchmark_id}-r{repetition + 1}-{case.name}",
            )
            raw_rows.append(_row(effective_case, repetition + 1, summary))
    raw_path = output_dir / "benchmark_runs.csv"
    _write_rows(raw_path, raw_rows)
    aggregate_path = output_dir / "benchmark_summary.csv"
    _write_rows(aggregate_path, _aggregate(raw_rows))
    plot_benchmark(aggregate_path, output_dir / "benchmark_comparison.png")
    return aggregate_path


def _copy_catalog(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(source) as source_db, sqlite3.connect(destination) as destination_db:
        source_db.backup(destination_db)


def _row(case: BenchmarkCase, repetition: int, summary: RunSummary) -> dict[str, Any]:
    return {
        "case": case.name,
        "repetition": repetition,
        "run_id": summary.run_id,
        "workload_tag": summary.workload_tag,
        "patch_size": case.patch_size,
        "workers": case.workers,
        "endpoint": "highvolume" if case.high_volume else "standard",
        "grouped": case.grouped,
        "metadata_workers": case.metadata_workers or "",
        "cloud_mode": case.cloud_mode or "hybrid_inline",
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
        "catalog_hit_rate": summary.catalog_metrics.get("catalog_hit_rate", ""),
        "metadata_queries": summary.catalog_metrics.get("compute_features_calls", ""),
        "metadata_seconds": summary.catalog_metrics.get("metadata_seconds", ""),
        "planning_seconds": summary.catalog_metrics.get("planning_seconds", ""),
        "quality_rejections": summary.catalog_metrics.get("quality_rejections", ""),
        "compute_pixels_p50_seconds": _timing_percentile(summary, "compute_pixels", 0.50),
        "compute_pixels_p95_seconds": _timing_percentile(summary, "compute_pixels", 0.95),
        "local_qa_p50_seconds": _timing_percentile(summary, "local_qa_and_split", 0.50),
    }


def _timing_percentile(summary: RunSummary, name: str, quantile: float) -> float | str:
    values = sorted(
        float(result.timings[name]) for result in summary.results if name in result.timings
    )
    if not values:
        return ""
    index = round((len(values) - 1) * quantile)
    return values[index]


def refresh_benchmark_eecu(raw_path: str | Path, project: str) -> Path:
    """Refresh delayed completed-EECU values and regenerate aggregate outputs."""
    raw_path = Path(raw_path)
    with raw_path.open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    if not rows:
        raise ValueError("Benchmark CSV is empty")
    if "case" not in rows[0]:
        for row in rows:
            snapshot = CloudEECUReader(project, row["workload_tag"]).read()
            row["completed_eecu_seconds"] = snapshot.completed_seconds
            samples = int(row["samples"])
            row["eecu_per_sample"] = snapshot.completed_seconds / samples if samples else ""
        _write_rows(raw_path, rows)
        return raw_path
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
                "grouped": first.get("grouped", ""),
                "metadata_workers": first.get("metadata_workers", ""),
                "cloud_mode": first.get("cloud_mode", ""),
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
                "catalog_hit_rate": _mean_present(group, "catalog_hit_rate"),
                "metadata_queries": _mean_present(group, "metadata_queries"),
                "metadata_seconds": _mean_present(group, "metadata_seconds"),
                "planning_seconds": _mean_present(group, "planning_seconds"),
                "quality_rejections": _mean_present(group, "quality_rejections"),
                "compute_pixels_p50_seconds": _mean_present(group, "compute_pixels_p50_seconds"),
                "compute_pixels_p95_seconds": _mean_present(group, "compute_pixels_p95_seconds"),
                "local_qa_p50_seconds": _mean_present(group, "local_qa_p50_seconds"),
            }
        )
    return result


def _mean_present(rows: Sequence[dict[str, Any]], key: str) -> float | str:
    values = [float(row[key]) for row in rows if row.get(key) not in (None, "")]
    return sum(values) / len(values) if values else ""


def staged_benchmark_patch_downloads(
    config: SamplerConfig,
    records: Iterable[SampleRecord],
    collection_builder_factory: Callable[[PatchGrid], Callable[[SampleRecord], Any]],
    *,
    bands: Sequence[str],
    output_dir: str | Path,
    selection: SceneSelection = DEFAULT_SCENE_SELECTION,
    cases: Sequence[BenchmarkCase] = DEFAULT_CASES,
    screen_sample_count: int = 32,
    screen_repetitions: int = 2,
    finalist_count: int = 4,
    final_sample_count: int = 128,
    final_repetitions: int = 3,
) -> Path:
    """Screen a broad matrix cheaply, then repeat only retained cases at full size."""
    samples = list(records)
    if not cases:
        raise ValueError("No benchmark cases")
    output_dir = Path(output_dir)
    benchmark_catalog_sync(
        config,
        samples[:screen_sample_count],
        output_dir=output_dir / "catalog-workers",
        selection=selection,
        grid=PatchGrid(max(case.patch_size for case in cases), 10),
    )
    screen = benchmark_patch_downloads(
        config,
        samples,
        collection_builder_factory,
        bands=bands,
        output_dir=output_dir / "screen",
        selection=selection,
        cases=cases,
        repetitions=screen_repetitions,
        sample_count=screen_sample_count,
    )
    with screen.open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    retained_names = _retain_cases(rows, finalist_count)
    retained = [case for case in cases if case.name in retained_names]
    (output_dir / "selection.json").write_text(
        json.dumps(
            {
                "screen_summary": str(screen),
                "retained_cases": retained_names,
                "criteria": [
                    "samples_per_second descending",
                    "bandwidth_mib_per_second descending",
                    "compute_pixels_p95_seconds ascending",
                    "eecu_per_success ascending when available",
                ],
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return benchmark_patch_downloads(
        config,
        samples,
        collection_builder_factory,
        bands=bands,
        output_dir=output_dir / "final",
        selection=selection,
        cases=retained,
        repetitions=final_repetitions,
        sample_count=final_sample_count,
    )


def benchmark_catalog_sync(
    config: SamplerConfig,
    records: Iterable[SampleRecord],
    *,
    output_dir: str | Path,
    selection: SceneSelection = DEFAULT_SCENE_SELECTION,
    grid: PatchGrid | None = None,
    metadata_workers: Sequence[int] = (1, 2, 4),
) -> Path | None:
    """Benchmark cold metadata synchronization separately from warm pixel downloads."""
    if config.catalog is None:
        return None
    grid = grid or PatchGrid(512, 10)
    samples = list(records)
    if not samples:
        raise ValueError("No catalog benchmark samples")
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    ee = initialize_earth_engine(replace(config.auth, high_volume=False))
    rows = []
    benchmark_id = datetime.now(timezone.utc).strftime("%H%M%S") + "-" + uuid.uuid4().hex[:4]
    for workers in metadata_workers:
        resolver = S2CatalogResolver(
            S2SceneCatalog(output_dir / f"{benchmark_id}-metadata-w{workers}.sqlite"),
            ee_module=ee,
            config=replace(
                config.catalog.resolver,
                metadata_workers=workers,
                cloud_mode="metadata_only",
            ),
        )
        started = datetime.now(timezone.utc)
        workload_tag = make_workload_tag(
            "geesampler", "catalog-benchmark", f"{benchmark_id}-w{workers}"
        )
        resolver.prepare(
            samples,
            grid=grid,
            selection=selection,
            workload_tag=workload_tag,
        )
        elapsed = (datetime.now(timezone.utc) - started).total_seconds()
        stats = resolver.stats()
        rows.append(
            {
                "metadata_workers": workers,
                "samples": len(samples),
                "elapsed_seconds": elapsed,
                "samples_per_second": len(samples) / elapsed if elapsed else 0,
                "compute_features_calls": stats.compute_features_calls,
                "metadata_rows": stats.metadata_rows,
                "metadata_seconds": stats.metadata_seconds,
                "catalog_hit_rate": stats.catalog_hit_rate,
                "workload_tag": workload_tag,
                "completed_eecu_seconds": "",
                "eecu_per_sample": "",
            }
        )
    output = output_dir / "catalog_worker_benchmark.csv"
    _write_rows(output, rows)
    return output


def _retain_cases(rows: Sequence[Mapping[str, Any]], count: int) -> list[str]:
    if count <= 0:
        raise ValueError("finalist_count must be positive")
    valid = [row for row in rows if int(float(row.get("failed", 0))) == 0]
    if not valid:
        valid = list(rows)
    rankings = (
        sorted(valid, key=lambda row: float(row["samples_per_second"]), reverse=True),
        sorted(valid, key=lambda row: float(row["bandwidth_mib_per_second"]), reverse=True),
        sorted(
            valid,
            key=lambda row: float(row.get("compute_pixels_p95_seconds") or "inf"),
        ),
        sorted(valid, key=lambda row: float(row.get("eecu_per_success") or "inf")),
    )
    retained: list[str] = []
    offset = 0
    while len(retained) < min(count, len(valid)):
        for ranking in rankings:
            if offset >= len(ranking):
                continue
            name = str(ranking[offset]["case"])
            if name not in retained:
                retained.append(name)
                if len(retained) == min(count, len(valid)):
                    break
        offset += 1
    return retained


def _write_rows(path: Path, rows: Sequence[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
