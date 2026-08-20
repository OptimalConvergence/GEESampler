from pathlib import Path

from geesampler.config import AccountProfile, DistributedRunConfig, SamplerConfig
from geesampler.distributed import (
    DistributedSampler,
    ProfileOutcome,
    assign_records,
    effective_workers,
)
from geesampler.models import (
    AuthConfig,
    EECUMonitorConfig,
    RunConfig,
    RunSummary,
    SampleRecord,
    TaskResult,
)


def _profile(name: str, project: str, workers: int) -> AccountProfile:
    return AccountProfile(name, AuthConfig(project), workers)


def _config(tmp_path: Path, profiles: tuple[AccountProfile, ...]) -> SamplerConfig:
    return SamplerConfig(
        profiles[0].auth,
        RunConfig(tmp_path, eecu=EECUMonitorConfig(enabled=False)),
        None,
        {},
        accounts=profiles,
        distributed=DistributedRunConfig(enabled=True, max_inflight_per_project=16),
    )


def test_project_cap_allocates_workers_across_profiles():
    profiles = (_profile("first", "shared", 12), _profile("second", "shared", 12))
    assert effective_workers(profiles, 16) == {"first": 8, "second": 8}


def test_affinity_assignment_is_complete_and_balanced():
    profiles = (_profile("first", "shared", 8), _profile("second", "shared", 8))
    records = [
        SampleRecord(str(index), {"type": "Point", "coordinates": [index / 10, 45]})
        for index in range(16)
    ]
    assigned = assign_records(records, profiles, {"first": 8, "second": 8})
    assert {item.sample_id for values in assigned.values() for item in values} == {
        item.sample_id for item in records
    }
    assert abs(len(assigned["first"]) - len(assigned["second"])) <= 4


def test_same_project_eecu_is_not_double_counted(tmp_path):
    profiles = (_profile("first", "shared", 8), _profile("second", "shared", 8))
    sampler = DistributedSampler(_config(tmp_path, profiles))
    records = [
        SampleRecord("a", {"type": "Point", "coordinates": [0, 0]}),
        SampleRecord("b", {"type": "Point", "coordinates": [1, 0]}),
    ]

    def summary(name: str, result: TaskResult, eecu: float) -> RunSummary:
        return RunSummary(
            name,
            "shared-tag",
            1,
            1,
            0,
            0,
            result.bytes_downloaded,
            1,
            1,
            1,
            retained_bytes=result.retained_bytes,
            useful_bandwidth_mib_per_second=0.5,
            completed_eecu_seconds=eecu,
            results=(result,),
        )

    first = TaskResult("a", "a", "success", "a.tif", 10, 1, 1, retained_bytes=6)
    second = TaskResult("b", "b", "success", "b.tif", 20, 1, 1, retained_bytes=12)
    aggregate = sampler._aggregate(
        "run",
        "scenario",
        records,
        [
            ProfileOutcome("first", "shared", 1, 8, summary("one", first, 5)),
            ProfileOutcome("second", "shared", 1, 8, summary("two", second, 7)),
        ],
        2,
    )
    assert aggregate.succeeded == 2
    assert aggregate.completed_eecu_seconds == 7
    assert aggregate.retained_bytes == 18
