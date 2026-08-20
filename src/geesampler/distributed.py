from __future__ import annotations

import json
import multiprocessing
import queue
import time
import uuid
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from functools import partial
from pathlib import Path
from typing import Any, Literal

from .config import AccountProfile, DistributedRunConfig, SamplerConfig, load_callable
from .engine import DownloadEngine, TaskLedger, make_workload_tag, redact_error
from .models import (
    DEFAULT_PATCH_GRID,
    DEFAULT_SCENE_SELECTION,
    PatchGrid,
    RunSummary,
    SampleRecord,
    SceneSelection,
)
from .sampler import Sampler

_NON_RETRYABLE_ERRORS = (
    "No qualifying scenes",
    "No metadata-qualified scenes",
    "No scene met inline cloud quality",
    "No scene met Cloud Score quality",
    "No catalog candidate was present in preprocessed collection",
    "quality_rejected",
    "EECU scheduling ceiling reached",
)


@dataclass(frozen=True)
class ProfileOutcome:
    profile_name: str
    project: str
    assigned: int
    workers: int
    summary: RunSummary | None = None
    fatal_error: str | None = None


def _profile_entry(
    output_queue: Any,
    profile: AccountProfile,
    workers: int,
    base_config: SamplerConfig,
    records: Sequence[SampleRecord],
    builder_path: str,
    builder_kwargs: Mapping[str, Any],
    bands: Sequence[str],
    kind: Literal["patch", "point"],
    grid: PatchGrid,
    scale: float,
    selection: SceneSelection,
    mask_builder_path: str | None,
    scenario: str,
    run_id: str,
    run_root: Path,
) -> None:
    try:
        run = replace(
            base_config.run,
            output_dir=run_root / "accounts" / profile.name,
            workers=workers,
        )
        config = replace(
            base_config,
            auth=profile.auth,
            run=run,
            accounts=(),
            distributed=DistributedRunConfig(),
        )
        sampler = Sampler(config)
        builder = partial(load_callable(builder_path), **dict(builder_kwargs))
        if kind == "patch":
            mask_builder = load_callable(mask_builder_path) if mask_builder_path else None
            summary = sampler.download_patch_series(
                records,
                builder,
                bands=bands,
                grid=grid,
                selection=selection,
                mask_builder=mask_builder,
                scenario=scenario,
                run_id=run_id,
            )
        else:
            summary = sampler.download_point_series(
                records,
                builder,
                bands=bands,
                scale=scale,
                selection=selection,
                scenario=scenario,
                run_id=run_id,
            )
        output_queue.put(
            ProfileOutcome(profile.name, profile.auth.project, len(records), workers, summary)
        )
    except BaseException as exc:  # noqa: BLE001 - isolate the credential process
        message = redact_error(exc)
        if profile.auth.key_file:
            message = message.replace(str(profile.auth.key_file), "<credential-file>")
        message = message.replace(profile.auth.project, "<project>")
        output_queue.put(
            ProfileOutcome(
                profile.name,
                profile.auth.project,
                len(records),
                workers,
                fatal_error=message,
            )
        )


def _centroid(sample: SampleRecord) -> tuple[float, float]:
    coordinates = sample.geometry["coordinates"]
    if sample.geometry["type"] == "Point":
        return float(coordinates[0]), float(coordinates[1])

    points: list[tuple[float, float]] = []

    def visit(value: Any) -> None:
        if (
            isinstance(value, Sequence)
            and len(value) >= 2
            and isinstance(value[0], (int, float))
            and isinstance(value[1], (int, float))
        ):
            points.append((float(value[0]), float(value[1])))
            return
        if isinstance(value, (str, bytes)):
            return
        for item in value:
            visit(item)

    visit(coordinates)
    if not points:
        raise ValueError(f"Sample {sample.sample_id} has no coordinate pairs")
    return (
        sum(point[0] for point in points) / len(points),
        sum(point[1] for point in points) / len(points),
    )


def affinity_key(sample: SampleRecord) -> tuple[int, int, int]:
    lon, lat = _centroid(sample)
    month = sample.date.year * 12 + sample.date.month if sample.date else 0
    return round(lon * 2), round(lat * 2), month


def effective_workers(
    profiles: Sequence[AccountProfile], max_inflight_per_project: int
) -> dict[str, int]:
    """Allocate a project cap proportionally without giving any profile zero workers."""
    result: dict[str, int] = {}
    by_project: dict[str, list[AccountProfile]] = defaultdict(list)
    for profile in profiles:
        by_project[profile.auth.project].append(profile)
    for project_profiles in by_project.values():
        if len(project_profiles) > max_inflight_per_project:
            raise ValueError("Project concurrency cap is lower than its enabled account count")
        allocated = {profile.name: 1 for profile in project_profiles}
        remaining = max_inflight_per_project - len(project_profiles)
        while remaining:
            eligible = [
                profile
                for profile in project_profiles
                if allocated[profile.name] < profile.workers
            ]
            if not eligible:
                break
            selected = min(
                eligible,
                key=lambda profile: (allocated[profile.name] / profile.workers, profile.name),
            )
            allocated[selected.name] += 1
            remaining -= 1
        result.update(allocated)
    return result


def assign_records(
    records: Sequence[SampleRecord],
    profiles: Sequence[AccountProfile],
    workers: Mapping[str, int],
    *,
    group_size: int = 4,
) -> dict[str, list[SampleRecord]]:
    ordered = sorted(records, key=lambda sample: (affinity_key(sample), sample.sample_id))
    assignments = {profile.name: [] for profile in profiles}
    for index in range(0, len(ordered), group_size):
        group = ordered[index : index + group_size]
        selected = min(
            profiles,
            key=lambda profile: (
                len(assignments[profile.name]) / workers[profile.name],
                profile.name,
            ),
        )
        assignments[selected.name].extend(group)
    return assignments


class DistributedSampler:
    """Run account-isolated samplers under a shared project-aware coordinator."""

    def __init__(self, config: SamplerConfig):
        if not config.distributed.enabled:
            raise ValueError("distributed.enabled must be true")
        self.config = config
        self.profiles = tuple(account for account in config.accounts if account.enabled)
        if len(self.profiles) < 2:
            raise ValueError("Distributed sampling requires at least two enabled accounts")
        self.workers = effective_workers(
            self.profiles, config.distributed.max_inflight_per_project
        )

    @classmethod
    def from_yaml(cls, path: str | Path) -> DistributedSampler:
        return cls(SamplerConfig.from_yaml(path))

    def download_patch_series(
        self,
        records: Iterable[SampleRecord],
        collection_builder: str,
        *,
        bands: Sequence[str],
        builder_kwargs: Mapping[str, Any] | None = None,
        grid: PatchGrid = DEFAULT_PATCH_GRID,
        selection: SceneSelection = DEFAULT_SCENE_SELECTION,
        mask_builder: str | None = None,
        scenario: str = "patches",
        run_id: str | None = None,
    ) -> RunSummary:
        return self._run(
            list(records),
            collection_builder,
            builder_kwargs or {},
            tuple(bands),
            "patch",
            grid,
            grid.scale,
            selection,
            mask_builder,
            scenario,
            run_id,
        )

    def download_point_series(
        self,
        records: Iterable[SampleRecord],
        collection_builder: str,
        *,
        bands: Sequence[str],
        builder_kwargs: Mapping[str, Any] | None = None,
        scale: float = 10,
        selection: SceneSelection = DEFAULT_SCENE_SELECTION,
        scenario: str = "points",
        run_id: str | None = None,
    ) -> RunSummary:
        return self._run(
            list(records),
            collection_builder,
            builder_kwargs or {},
            tuple(bands),
            "point",
            PatchGrid(1, scale),
            scale,
            selection,
            None,
            scenario,
            run_id,
        )

    def _run(
        self,
        records: list[SampleRecord],
        builder_path: str,
        builder_kwargs: Mapping[str, Any],
        bands: Sequence[str],
        kind: Literal["patch", "point"],
        grid: PatchGrid,
        scale: float,
        selection: SceneSelection,
        mask_builder_path: str | None,
        scenario: str,
        run_id: str | None,
    ) -> RunSummary:
        if not records:
            raise ValueError("No samples to distribute")
        started = time.monotonic()
        run_id = run_id or (
            datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]
        )
        run_root = self.config.run.output_dir / run_id
        run_root.mkdir(parents=True, exist_ok=True)
        assignments = assign_records(records, self.profiles, self.workers)
        outcomes = self._run_assignments(
            assignments,
            self.profiles,
            builder_path,
            builder_kwargs,
            bands,
            kind,
            grid,
            scale,
            selection,
            mask_builder_path,
            scenario,
            run_id,
            run_root,
        )
        all_outcomes = list(outcomes)
        retry_records = self._retry_records(records, assignments, outcomes)
        healthy = [profile for profile in self.profiles if self._healthy(profile.name, outcomes)]
        for _ in range(self.config.distributed.failover_attempts):
            if not retry_records or not healthy:
                break
            retry_assignments = assign_records(retry_records, healthy, self.workers)
            retry_outcomes = self._run_assignments(
                retry_assignments,
                healthy,
                builder_path,
                builder_kwargs,
                bands,
                kind,
                grid,
                scale,
                selection,
                mask_builder_path,
                scenario,
                run_id,
                run_root,
            )
            all_outcomes.extend(retry_outcomes)
            retry_records = self._retry_records(retry_records, retry_assignments, retry_outcomes)
            assigned_profiles = {
                name for name, profile_records in retry_assignments.items() if profile_records
            }
            healthy = [
                profile
                for profile in healthy
                if profile.name not in assigned_profiles
                or self._healthy(profile.name, retry_outcomes)
            ]
        summary = self._aggregate(run_id, scenario, records, all_outcomes, time.monotonic() - started)
        ledger = TaskLedger(run_root / "ledger.sqlite")
        for result in summary.results:
            ledger.record(result)
        ledger.close()
        DownloadEngine._write_manifest(run_root / "manifest.csv", summary.results)
        (run_root / "summary.json").write_text(
            json.dumps(summary.to_dict(), indent=2, sort_keys=True), encoding="utf-8"
        )
        return summary

    def _run_assignments(
        self,
        assignments: Mapping[str, Sequence[SampleRecord]],
        profiles: Sequence[AccountProfile],
        builder_path: str,
        builder_kwargs: Mapping[str, Any],
        bands: Sequence[str],
        kind: Literal["patch", "point"],
        grid: PatchGrid,
        scale: float,
        selection: SceneSelection,
        mask_builder_path: str | None,
        scenario: str,
        run_id: str,
        run_root: Path,
    ) -> list[ProfileOutcome]:
        context = multiprocessing.get_context("spawn")
        output_queue = context.Queue()
        processes: dict[str, Any] = {}
        for profile in profiles:
            profile_records = list(assignments.get(profile.name, ()))
            if not profile_records:
                continue
            process = context.Process(
                target=_profile_entry,
                args=(
                    output_queue,
                    profile,
                    self.workers[profile.name],
                    self.config,
                    profile_records,
                    builder_path,
                    builder_kwargs,
                    bands,
                    kind,
                    grid,
                    scale,
                    selection,
                    mask_builder_path,
                    scenario,
                    run_id,
                    run_root,
                ),
                name=f"geesampler-{profile.name}",
            )
            process.start()
            processes[profile.name] = process
        outcomes: dict[str, ProfileOutcome] = {}
        while len(outcomes) < len(processes):
            try:
                outcome = output_queue.get(timeout=0.5)
                outcomes[outcome.profile_name] = outcome
            except queue.Empty:
                if not any(process.is_alive() for process in processes.values()):
                    break
        for process in processes.values():
            process.join()
        for profile in profiles:
            if profile.name in processes and profile.name not in outcomes:
                outcomes[profile.name] = ProfileOutcome(
                    profile.name,
                    profile.auth.project,
                    len(assignments[profile.name]),
                    self.workers[profile.name],
                    fatal_error="Worker process exited without a result",
                )
        return list(outcomes.values())

    @staticmethod
    def _healthy(profile_name: str, outcomes: Sequence[ProfileOutcome]) -> bool:
        matching = [outcome for outcome in outcomes if outcome.profile_name == profile_name]
        return bool(matching) and all(outcome.fatal_error is None for outcome in matching)

    @staticmethod
    def _retry_records(
        records: Sequence[SampleRecord],
        assignments: Mapping[str, Sequence[SampleRecord]],
        outcomes: Sequence[ProfileOutcome],
    ) -> list[SampleRecord]:
        successful = {
            result.sample_id
            for outcome in outcomes
            if outcome.summary is not None
            for result in outcome.summary.results
            if result.status == "success"
        }
        fatal_profiles = {outcome.profile_name for outcome in outcomes if outcome.fatal_error}
        fatal_samples = {
            sample.sample_id
            for profile_name in fatal_profiles
            for sample in assignments.get(profile_name, ())
        }
        retryable_samples = set(fatal_samples)
        for outcome in outcomes:
            if outcome.summary is None:
                continue
            for result in outcome.summary.results:
                if result.status != "failed":
                    continue
                error = result.error or ""
                if not error.startswith(_NON_RETRYABLE_ERRORS):
                    retryable_samples.add(result.sample_id)
        return [
            sample
            for sample in records
            if sample.sample_id not in successful and sample.sample_id in retryable_samples
        ]

    def _aggregate(
        self,
        run_id: str,
        scenario: str,
        records: Sequence[SampleRecord],
        outcomes: Sequence[ProfileOutcome],
        elapsed: float,
    ) -> RunSummary:
        results = tuple(
            result
            for outcome in outcomes
            if outcome.summary is not None
            for result in outcome.summary.results
        )
        statuses: dict[str, set[str]] = defaultdict(set)
        for result in results:
            statuses[result.sample_id].add(result.status)
        succeeded = sum("success" in value for value in statuses.values())
        skipped = sum(value == {"skipped"} for value in statuses.values())
        failed = len(records) - succeeded - skipped
        bytes_downloaded = sum(result.bytes_downloaded for result in results)
        retained_bytes = sum(result.retained_bytes for result in results)
        completed_by_project: dict[str, float] = {}
        in_progress_by_project: dict[str, float] = {}
        for outcome in outcomes:
            if outcome.summary is not None:
                summary = outcome.summary
                if summary.completed_eecu_seconds is not None:
                    completed_by_project[outcome.project] = max(
                        completed_by_project.get(outcome.project, 0),
                        summary.completed_eecu_seconds,
                    )
                if summary.in_progress_eecu_seconds is not None:
                    in_progress_by_project[outcome.project] = max(
                        in_progress_by_project.get(outcome.project, 0),
                        summary.in_progress_eecu_seconds,
                    )
        profile_metrics: dict[str, Any] = {}
        for profile in self.profiles:
            profile_outcomes = [
                outcome for outcome in outcomes if outcome.profile_name == profile.name
            ]
            profile_summaries = [
                outcome.summary for outcome in profile_outcomes if outcome.summary is not None
            ]
            profile_results = [
                result for summary in profile_summaries for result in summary.results
            ]
            profile_successes = len(
                {result.sample_id for result in profile_results if result.status == "success"}
            )
            profile_elapsed = sum(summary.elapsed_seconds for summary in profile_summaries)
            profile_bytes = sum(result.bytes_downloaded for result in profile_results)
            profile_retained = sum(result.retained_bytes for result in profile_results)
            metrics = {
                "assigned": sum(outcome.assigned for outcome in profile_outcomes),
                "workers": self.workers[profile.name],
                "succeeded": profile_successes,
                "failed": len(
                    {
                        result.sample_id
                        for result in profile_results
                        if result.status == "failed"
                    }
                    - {
                        result.sample_id
                        for result in profile_results
                        if result.status == "success"
                    }
                ),
                "elapsed_seconds": profile_elapsed,
                "samples_per_second": (
                    profile_successes / profile_elapsed if profile_elapsed else 0
                ),
                "bandwidth_mib_per_second": (
                    profile_bytes / (1024**2) / profile_elapsed if profile_elapsed else 0
                ),
                "useful_bandwidth_mib_per_second": (
                    profile_retained / (1024**2) / profile_elapsed
                    if profile_elapsed
                    else 0
                ),
            }
            fatal_errors = [
                outcome.fatal_error for outcome in profile_outcomes if outcome.fatal_error
            ]
            if fatal_errors:
                metrics["fatal_error"] = "; ".join(fatal_errors)
            profile_metrics[profile.name] = metrics
        return RunSummary(
            run_id=run_id,
            workload_tag=make_workload_tag(self.config.run.workload_prefix, scenario, run_id),
            total=len(records),
            succeeded=succeeded,
            failed=failed,
            skipped=skipped,
            bytes_downloaded=bytes_downloaded,
            elapsed_seconds=elapsed,
            samples_per_second=succeeded / elapsed if elapsed else 0,
            bandwidth_mib_per_second=bytes_downloaded / (1024**2) / elapsed if elapsed else 0,
            retained_bytes=retained_bytes,
            useful_bandwidth_mib_per_second=(
                retained_bytes / (1024**2) / elapsed if elapsed else 0
            ),
            completed_eecu_seconds=sum(completed_by_project.values()) or None,
            in_progress_eecu_seconds=sum(in_progress_by_project.values()) or None,
            stopped_by_eecu_budget=any(
                outcome.summary and outcome.summary.stopped_by_eecu_budget
                for outcome in outcomes
            ),
            catalog_metrics={
                "distributed": True,
                "projects": len({profile.auth.project for profile in self.profiles}),
                "profiles": profile_metrics,
                "max_inflight_per_project": self.config.distributed.max_inflight_per_project,
            },
            results=results,
        )
