import csv

from geesampler import benchmark
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
