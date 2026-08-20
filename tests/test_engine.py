import time
from datetime import datetime, timezone
from types import SimpleNamespace

from geesampler.engine import DownloadEngine, make_workload_tag
from geesampler.models import (
    EECUMonitorConfig,
    PatchGrid,
    RunConfig,
    SampleRecord,
    SceneSelection,
    TaskResult,
)
from geesampler.monitoring import EECUSnapshot, SequenceEECUReader


class FakeImage:
    def select(self, _bands):
        return self


class FakeList:
    def __init__(self, items):
        self.items = items

    def get(self, index):
        return self.items[index]


class FakeSelected:
    def __init__(self):
        self.items = [FakeImage()]

    def toList(self, _limit):
        return FakeList(self.items)


class FakeData:
    def __init__(self):
        self.requests = []

    def computePixels(self, request):
        self.requests.append(request)
        return b"valid-geotiff-placeholder"


class FakeEE:
    def __init__(self):
        self.data = FakeData()

    @staticmethod
    def Image(value):
        return value


class StubEngine(DownloadEngine):
    def _select_scenes(self, collection, target, selection, workload_tag):
        assert workload_tag.startswith("geesampler-test-")
        return FakeSelected(), [{"scene_id": "S2_A", "scene_time": 1_625_097_600_000}]


def test_workload_tag_matches_live_api_contract():
    tag = make_workload_tag("GEE.Sampler", "Prepared S2", "Run.01")
    assert tag == "gee-sampler-prepared-s2-run-01"
    assert len(tag) <= 63


def test_patch_request_grid_tag_manifest_and_resume(tmp_path):
    fake = FakeEE()
    config = RunConfig(
        tmp_path,
        workers=2,
        retries=0,
        eecu=EECUMonitorConfig(enabled=False),
    )
    engine = StubEngine("project", config, ee_module=fake)
    sample = SampleRecord(
        "sample-1",
        {"type": "Point", "coordinates": [145.5, -37.5]},
        datetime(2021, 7, 1, tzinfo=timezone.utc),
        {"AGBD": 123.4},
    )
    summary = engine.download_patch_series(
        [sample],
        lambda _sample: object(),
        bands=["B2", "B3", "B4"],
        grid=PatchGrid(336, 10),
        selection=SceneSelection(),
        scenario="test",
        run_id="fixed",
    )
    assert summary.succeeded == 1
    request = fake.data.requests[0]
    assert request["workloadTag"] == "geesampler-test-fixed"
    assert request["grid"]["dimensions"] == {"width": 336, "height": 336}
    manifest = (tmp_path / "fixed" / "manifest.csv").read_text(encoding="utf-8")
    assert "AGBD" in manifest
    assert "S2_A" in manifest

    resumed = engine.download_patch_series(
        [sample],
        lambda _sample: object(),
        bands=["B2", "B3", "B4"],
        scenario="test",
        run_id="fixed",
    )
    assert resumed.skipped == 1
    assert len(fake.data.requests) == 1


def test_scheduler_stops_new_samples_at_eecu_ceiling(tmp_path):
    fake = FakeEE()
    config = RunConfig(
        tmp_path,
        workers=1,
        eecu=EECUMonitorConfig(
            enabled=True,
            poll_seconds=0.001,
            hard_eecu_hours=1,
        ),
    )

    def reader_factory(_project, _tag):
        return SequenceEECUReader([EECUSnapshot("now", 3600, 0)])

    engine = DownloadEngine("project", config, ee_module=fake, eecu_reader_factory=reader_factory)
    samples = [
        SampleRecord(str(index), {"type": "Point", "coordinates": [0, 0]}) for index in range(5)
    ]

    def worker(sample, tag, ledger, run_dir):
        time.sleep(0.02)
        return [TaskResult(sample.sample_id, sample.sample_id, "success", None, 1, 0.02, 1)]

    summary = engine._run(samples, "budget", "fixed", worker)
    assert summary.stopped_by_eecu_budget
    assert summary.succeeded < 5
    assert summary.skipped > 0


def test_summary_counts_samples_not_scene_files():
    results = [
        TaskResult("a-1", "a", "success", None, 10, 1, 1),
        TaskResult("a-2", "a", "success", None, 20, 1, 1),
        TaskResult("b-1", "b", "failed", None, 0, 1, 1),
    ]
    summary = DownloadEngine._summary(
        "run", "tag", 2, results, 2, SimpleNamespace(latest=None), False
    )
    assert summary.succeeded == 1
    assert summary.failed == 1
    assert summary.samples_per_second == 0.5
