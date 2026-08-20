import csv
from datetime import datetime, timezone

from geesampler import benchmark
from geesampler.catalog import S2SceneCatalog, SceneRecord
from geesampler.monitoring import EECUSnapshot


def test_refresh_benchmark_eecu(tmp_path, monkeypatch):
    raw = tmp_path / "benchmark_runs.csv"
    rows = [
        {
            "case": "case-a",
            "repetition": 1,
            "run_id": "suite-r1-case-a",
            "workload_tag": "tag-a",
            "patch_size": 336,
            "workers": 8,
            "endpoint": "standard",
            "successful": 2,
            "failed": 0,
            "elapsed_seconds": 1,
            "bandwidth_mib_per_second": 3,
            "samples_per_second": 2,
            "completed_eecu_seconds": 0,
            "eecu_per_success": 0,
        }
    ]
    benchmark._write_rows(raw, rows)

    class Reader:
        def __init__(self, project, workload_tag):
            assert (project, workload_tag) == ("project", "tag-a")

        def read(self):
            return EECUSnapshot("now", 6, 0)

    monkeypatch.setattr(benchmark, "CloudEECUReader", Reader)
    monkeypatch.setattr(benchmark, "plot_benchmark", lambda *_args: None)
    summary = benchmark.refresh_benchmark_eecu(raw, "project")
    with raw.open(newline="", encoding="utf-8") as stream:
        refreshed = next(csv.DictReader(stream))
    assert refreshed["completed_eecu_seconds"] == "6"
    assert refreshed["eecu_per_success"] == "3.0"
    assert summary == tmp_path / "benchmark_summary.csv"


def test_refresh_catalog_benchmark_eecu(tmp_path, monkeypatch):
    raw = tmp_path / "catalog_worker_benchmark.csv"
    benchmark._write_rows(
        raw,
        [
            {
                "metadata_workers": 2,
                "samples": 3,
                "workload_tag": "catalog-tag",
                "completed_eecu_seconds": "",
                "eecu_per_sample": "",
            }
        ],
    )

    class Reader:
        def __init__(self, project, workload_tag):
            assert (project, workload_tag) == ("project", "catalog-tag")

        def read(self):
            return EECUSnapshot("now", 6, 0)

    monkeypatch.setattr(benchmark, "CloudEECUReader", Reader)
    assert benchmark.refresh_benchmark_eecu(raw, "project") == raw
    with raw.open(newline="", encoding="utf-8") as stream:
        refreshed = next(csv.DictReader(stream))
    assert refreshed["eecu_per_sample"] == "2.0"


def test_retain_cases_uses_distinct_performance_objectives():
    rows = [
        {
            "case": "fast-samples",
            "failed": 0,
            "samples_per_second": 5,
            "bandwidth_mib_per_second": 1,
            "compute_pixels_p95_seconds": 4,
            "eecu_per_success": 3,
        },
        {
            "case": "fast-bytes",
            "failed": 0,
            "samples_per_second": 2,
            "bandwidth_mib_per_second": 8,
            "compute_pixels_p95_seconds": 3,
            "eecu_per_success": 2,
        },
        {
            "case": "low-tail",
            "failed": 0,
            "samples_per_second": 1,
            "bandwidth_mib_per_second": 2,
            "compute_pixels_p95_seconds": 1,
            "eecu_per_success": 1,
        },
    ]
    assert benchmark._retain_cases(rows, 3) == ["fast-samples", "fast-bytes", "low-tail"]


def test_benchmark_catalog_copy_preserves_metadata_without_sharing_quality(tmp_path):
    source_path = tmp_path / "source.sqlite"
    source = S2SceneCatalog(source_path)
    scene = SceneRecord(
        "collection", "asset", "scene", 1_600_000_000_000, "31TCJ", 5, (-1, -1, 1, 1)
    )
    source.upsert_and_mark_coverage(
        [scene],
        collection="collection",
        tiles={"31TCJ"},
        start=datetime(2020, 1, 1, tzinfo=timezone.utc),
        end=datetime(2021, 1, 1, tzinfo=timezone.utc),
    )
    source.record_quality(scene, "grid", "cs_cdf", 0.6, 0.8, 0.9, True)
    destination_path = tmp_path / "destination.sqlite"
    benchmark._copy_catalog(source_path, destination_path)
    benchmark._clear_catalog_quality(destination_path)
    destination = S2SceneCatalog(destination_path)
    assert destination.stats().scenes == 1
    assert destination.stats().patch_quality_rows == 0


def test_patch_payload_estimate_enforces_compute_pixels_limit():
    assert benchmark.estimate_uncompressed_mib(1536, 6) < 48
    assert benchmark.estimate_uncompressed_mib(1792, 6) > 48
