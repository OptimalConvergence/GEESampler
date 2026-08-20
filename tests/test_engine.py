import json
import time
from datetime import datetime, timezone
from types import SimpleNamespace

import numpy as np
import rasterio
from rasterio.io import MemoryFile

from geesampler.catalog import SceneRecord
from geesampler.engine import DownloadEngine, make_workload_tag, redact_error
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


def test_error_redaction_removes_credential_shapes():
    message = redact_error(
        "account=name@example.iam.gserviceaccount.com Authorization: Bearer abc.def "
        "-----BEGIN PRIVATE KEY-----\nsecret\n-----END PRIVATE KEY-----"
    )
    assert "name@example" not in message
    assert "abc.def" not in message
    assert "secret" not in message


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


class ResolvedImage:
    def select(self, _bands):
        return self

    def addBands(self, _band):
        return self

    def unmask(self, _value):
        return self

    def uint8(self):
        return self


class ResolvedCollection:
    def filter(self, _filter):
        return self

    def first(self):
        return ResolvedImage()


class ResolvedFilter:
    @staticmethod
    def eq(_name, _value):
        return object()


def _quality_tiff(clear: bool) -> bytes:
    profile = {
        "driver": "GTiff",
        "width": 4,
        "height": 4,
        "count": 2,
        "dtype": "uint16",
        "crs": "EPSG:4326",
        "transform": rasterio.transform.from_origin(0, 4, 1, 1),
    }
    with MemoryFile() as memory:
        with memory.open(**profile) as dataset:
            dataset.write(np.full((4, 4), 100, dtype="uint16"), 1)
            dataset.write(np.full((4, 4), int(clear), dtype="uint16"), 2)
        return memory.read()


def _single_band_tiff(clear: bool) -> bytes:
    profile = {
        "driver": "GTiff",
        "width": 4,
        "height": 4,
        "count": 1,
        "dtype": "uint16",
        "crs": "EPSG:4326",
        "transform": rasterio.transform.from_origin(0, 4, 1, 1),
    }
    with MemoryFile() as memory:
        with memory.open(**profile) as dataset:
            dataset.write(np.full((4, 4), int(clear), dtype="uint16"), 1)
        return memory.read()


class ResolvedData:
    def __init__(self):
        self.requests = []
        self.payloads = [_quality_tiff(False), _quality_tiff(True)]

    def computePixels(self, request):
        self.requests.append(request)
        return self.payloads.pop(0)


class ResolvedEE:
    Filter = ResolvedFilter

    def __init__(self):
        self.data = ResolvedData()

    @staticmethod
    def Image(value):
        return value


class ResolvedResolver:
    inline_quality = True
    internal_quality_band = "geesampler_clear"
    min_clear_fraction = 0.8

    def __init__(self):
        self.recorded = []
        self._samples = {}
        self._candidates = [
            SceneRecord("c", "a", "scene-a", 1_625_097_600_000, "31TCJ", 1),
            SceneRecord("c", "b", "scene-b", 1_625_184_000_000, "31TCJ", 2),
        ]

    def prepare(self, samples, **_kwargs):
        self._samples = {sample.sample_id: list(self._candidates) for sample in samples}

    @staticmethod
    def order_samples(samples):
        return list(samples)

    def candidates(self, sample):
        return self._samples[sample.sample_id]

    @staticmethod
    def cached_quality(_sample, _scene):
        return None

    @staticmethod
    def apply_inline_quality(image, *, include_band):
        return image, image if include_band else None

    def record_quality(self, _sample, scene, fraction, accepted):
        self.recorded.append((scene.scene_id, fraction, accepted))

    @staticmethod
    def stats():
        return SimpleNamespace(
            catalog_hits=1,
            catalog_misses=0,
            compute_features_calls=0,
            metadata_rows=0,
            metadata_seconds=0.0,
            lookup_seconds=0.0,
            quality_cache_hits=0,
            quality_rejections=1,
            catalog_hit_rate=1.0,
        )


def test_resolved_inline_quality_rejects_then_accepts_without_compute_features(tmp_path):
    fake = ResolvedEE()
    resolver = ResolvedResolver()
    engine = DownloadEngine(
        "project",
        RunConfig(tmp_path, workers=2, retries=0, eecu=EECUMonitorConfig(enabled=False)),
        ee_module=fake,
    )
    sample = SampleRecord(
        "sample-1",
        {"type": "Point", "coordinates": [3, 45]},
        datetime(2021, 7, 1, tzinfo=timezone.utc),
    )
    summary = engine.download_patch_series(
        [sample],
        lambda _sample: ResolvedCollection(),
        bands=["B2"],
        grid=PatchGrid(4, 10),
        scene_resolver=resolver,
        run_id="inline",
    )
    assert summary.succeeded == 1
    assert len(fake.data.requests) == 2
    assert resolver.recorded == [("scene-a", 0.0, False), ("scene-b", 1.0, True)]
    assert summary.results[0].bytes_downloaded == sum(
        len(payload) for payload in (_quality_tiff(False), _quality_tiff(True))
    )
    with rasterio.open(summary.results[0].output_path) as output:
        assert output.count == 1
    assert summary.catalog_metrics["catalog_hit_rate"] == 1.0
    profile = json.loads((tmp_path / "inline" / "profile.json").read_text(encoding="utf-8"))
    assert profile["steps"]["compute_pixels"]["p95_seconds"] >= 0


class ProbeResolver(ResolvedResolver):
    inline_quality = False
    probe_quality = True

    @staticmethod
    def quality_image(image):
        return image


class MetadataResolver(ResolvedResolver):
    inline_quality = False
    probe_quality = False
    cloud_masking = False


def test_hybrid_probe_rejects_with_one_band_before_full_download(tmp_path):
    fake = ResolvedEE()
    rejected = _single_band_tiff(False)
    accepted = _single_band_tiff(True)
    data = _single_band_tiff(True)
    fake.data.payloads = [rejected, accepted, data]
    resolver = ProbeResolver()
    engine = DownloadEngine(
        "project",
        RunConfig(tmp_path, workers=2, retries=0, eecu=EECUMonitorConfig(enabled=False)),
        ee_module=fake,
    )
    sample = SampleRecord(
        "sample-1",
        {"type": "Point", "coordinates": [3, 45]},
        datetime(2021, 7, 1, tzinfo=timezone.utc),
    )
    summary = engine.download_patch_series(
        [sample],
        lambda _sample: ResolvedCollection(),
        bands=["B2"],
        grid=PatchGrid(4, 10),
        scene_resolver=resolver,
        run_id="probe",
    )
    assert summary.succeeded == 1
    assert [request["bandIds"] for request in fake.data.requests] == [
        ["geesampler_clear"],
        ["geesampler_clear"],
        ["B2"],
    ]
    assert resolver.recorded == [("scene-a", 0.0, False), ("scene-b", 1.0, True)]
    assert summary.bytes_downloaded == sum(map(len, (rejected, accepted, data)))
    assert summary.retained_bytes > 0
    assert summary.useful_bandwidth_mib_per_second > 0


def test_missing_preprocessed_candidate_falls_back_to_next_scene(tmp_path):
    fake = ResolvedEE()

    class FallbackData:
        def __init__(self):
            self.requests = []

        def computePixels(self, request):
            self.requests.append(request)
            if len(self.requests) == 1:
                raise ValueError("Image.select: Parameter 'input' is required and may not be null.")
            return _single_band_tiff(True)

    fake.data = FallbackData()
    resolver = MetadataResolver()
    engine = DownloadEngine(
        "project",
        RunConfig(tmp_path, workers=1, retries=0, eecu=EECUMonitorConfig(enabled=False)),
        ee_module=fake,
    )
    sample = SampleRecord(
        "sample-1",
        {"type": "Point", "coordinates": [3, 45]},
        datetime(2021, 7, 1, tzinfo=timezone.utc),
    )
    summary = engine.download_patch_series(
        [sample],
        lambda _sample: ResolvedCollection(),
        bands=["B2"],
        grid=PatchGrid(4, 10),
        scene_resolver=resolver,
        run_id="fallback",
    )
    assert summary.succeeded == 1
    assert len(fake.data.requests) == 2
    assert summary.results[0].scene_id == "scene-b"
    assert summary.results[0].attempts == 2
