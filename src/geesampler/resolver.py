from __future__ import annotations

import hashlib
import json
import logging
import random
import threading
import time
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

from .catalog import S2SceneCatalog, SceneRecord
from .grid import ComputedGrid, compute_grid
from .models import PatchGrid, SampleRecord, SceneSelection

LOGGER = logging.getLogger(__name__)
S2_COLLECTION = "COPERNICUS/S2_SR_HARMONIZED"
CLOUD_SCORE_COLLECTION = "GOOGLE/CLOUD_SCORE_PLUS/V1/S2_HARMONIZED"
INLINE_CLEAR_BAND = "geesampler_clear"


@dataclass(frozen=True)
class CatalogResolverConfig:
    mode: Literal["read_through", "offline", "refresh"] = "read_through"
    metadata_workers: int = 2
    metadata_retries: int = 4
    retry_base_seconds: float = 1.0
    query_window_days: int = 366
    max_tiles_per_query: int = 8
    recent_horizon_days: int = 30
    recent_refresh_hours: int = 24
    metadata_cloud_max: float = 20.0
    cloud_mode: Literal["hybrid_inline", "hybrid_probe", "metadata_only"] = "hybrid_inline"
    qa_band: str = "cs_cdf"
    qa_threshold: float = 0.60
    min_clear_fraction: float = 0.80
    group_downloads: bool = True

    def __post_init__(self) -> None:
        if self.mode not in {"read_through", "offline", "refresh"}:
            raise ValueError(f"Unknown catalog mode: {self.mode}")
        if self.cloud_mode not in {"hybrid_inline", "hybrid_probe", "metadata_only"}:
            raise ValueError(f"Unknown catalog cloud mode: {self.cloud_mode}")
        if self.metadata_workers <= 0:
            raise ValueError("catalog metadata_workers must be positive")
        if self.metadata_retries < 0 or self.retry_base_seconds < 0:
            raise ValueError("catalog retry settings must be non-negative")
        if self.query_window_days <= 0 or self.max_tiles_per_query <= 0:
            raise ValueError("catalog query limits must be positive")
        if not 0 <= self.metadata_cloud_max <= 100:
            raise ValueError("metadata_cloud_max must be in [0, 100]")
        for name in ("qa_threshold", "min_clear_fraction"):
            if not 0 <= getattr(self, name) <= 1:
                raise ValueError(f"{name} must be in [0, 1]")


@dataclass(frozen=True)
class ResolverStats:
    catalog_hits: int = 0
    catalog_misses: int = 0
    compute_features_calls: int = 0
    metadata_rows: int = 0
    metadata_seconds: float = 0.0
    lookup_seconds: float = 0.0
    quality_cache_hits: int = 0
    quality_rejections: int = 0

    @property
    def catalog_hit_rate(self) -> float:
        total = self.catalog_hits + self.catalog_misses
        return self.catalog_hits / total if total else 1.0


def _features(result: Any) -> list[Mapping[str, Any]]:
    if isinstance(result, Mapping):
        return list(result.get("features", []))
    return list(result)


def _millis(value: datetime) -> int:
    normalized = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return int(normalized.timestamp() * 1000)


def _datetime(value: int) -> datetime:
    return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc)


def _chunks(values: Sequence[str], size: int) -> Iterable[Sequence[str]]:
    for index in range(0, len(values), size):
        yield values[index : index + size]


class MGRSTileLocator:
    """Resolve the center and corners of an output grid to Sentinel-2 MGRS tiles."""

    def __init__(self) -> None:
        try:
            import mgrs
        except ImportError as exc:  # pragma: no cover - dependency error is explicit
            raise ImportError("S2 catalog resolution requires the 'mgrs' package") from exc
        self._mgrs = mgrs.MGRS()

    def __call__(self, grid: ComputedGrid) -> tuple[set[str], tuple[float, float, float, float]]:
        from pyproj import Transformer

        transformer = Transformer.from_crs(grid.crs, "EPSG:4326", always_xy=True)
        left = grid.translate_x
        right = left + grid.width * grid.scale_x
        top = grid.translate_y
        bottom = top + grid.height * grid.scale_y
        center_x, center_y = (left + right) / 2.0, (top + bottom) / 2.0
        projected = (
            (left, top),
            (right, top),
            (right, bottom),
            (left, bottom),
            (center_x, center_y),
            (center_x, top),
            (center_x, bottom),
            (left, center_y),
            (right, center_y),
        )
        lon_lat = [transformer.transform(x, y) for x, y in projected]
        lookup_points = list(lon_lat)
        # Sentinel-2 MGRS footprints overlap their nominal UTM zone and latitude-band
        # boundaries. Include codes immediately across nearby boundaries so a patch
        # covered by an adjacent-zone tile is not missed on a cold catalog.
        for lon, lat in lon_lat:
            zone_boundary = round((lon + 180.0) / 6.0) * 6.0 - 180.0
            if -180.0 < zone_boundary < 180.0 and abs(lon - zone_boundary) <= 0.75:
                lookup_points.extend(((zone_boundary - 0.0001, lat), (zone_boundary + 0.0001, lat)))
            band_boundaries = [*range(-80, 73, 8), 84]
            band_boundary = min(band_boundaries, key=lambda value: abs(lat - value))
            if -80 < band_boundary < 84 and abs(lat - band_boundary) <= 0.75:
                lookup_points.extend(((lon, band_boundary - 0.0001), (lon, band_boundary + 0.0001)))
        tiles = {
            str(self._mgrs.toMGRS(lat, lon, MGRSPrecision=0))[:5]
            for lon, lat in lookup_points
            if -80 <= lat <= 84
        }
        if not tiles:
            raise ValueError("Sentinel-2 MGRS catalog supports latitudes from 80°S to 84°N")
        lons, lats = zip(*lon_lat[:4])
        return tiles, (min(lons), min(lats), max(lons), max(lats))


class S2CatalogResolver:
    """Resolve S2 candidates locally and fill missing metadata in grouped EE calls."""

    def __init__(
        self,
        catalog: S2SceneCatalog,
        *,
        ee_module: Any,
        config: CatalogResolverConfig | None = None,
        collection: str = S2_COLLECTION,
        tile_locator: Callable[[ComputedGrid], tuple[set[str], tuple[float, float, float, float]]]
        | None = None,
        metadata_fetcher: Callable[[Sequence[str], datetime, datetime, str], Sequence[SceneRecord]]
        | None = None,
    ):
        self.catalog = catalog
        self.ee = ee_module
        self.config = config or CatalogResolverConfig()
        self.collection = collection
        self._tile_locator = tile_locator or MGRSTileLocator()
        self._metadata_fetcher = metadata_fetcher or self._fetch_metadata
        self._plans: dict[str, list[SceneRecord]] = {}
        self._grids: dict[str, ComputedGrid] = {}
        self._tiles: dict[str, set[str]] = {}
        self._bboxes: dict[str, tuple[float, float, float, float]] = {}
        self._quality: dict[tuple[str, str], tuple[float, bool] | None] = {}
        self._stats = ResolverStats()
        self._lock = threading.Lock()

    @property
    def inline_quality(self) -> bool:
        return self.config.cloud_mode == "hybrid_inline"

    @property
    def probe_quality(self) -> bool:
        return self.config.cloud_mode == "hybrid_probe"

    @property
    def cloud_masking(self) -> bool:
        return self.config.cloud_mode != "metadata_only"

    @property
    def internal_quality_band(self) -> str:
        return INLINE_CLEAR_BAND

    @property
    def min_clear_fraction(self) -> float:
        return self.config.min_clear_fraction

    def stats(self) -> ResolverStats:
        with self._lock:
            return ResolverStats(**asdict(self._stats))

    def _add_stats(self, **increments: float) -> None:
        with self._lock:
            values = asdict(self._stats)
            for key, increment in increments.items():
                values[key] += increment
            self._stats = ResolverStats(**values)

    @staticmethod
    def _requested_interval(
        sample: SampleRecord, selection: SceneSelection
    ) -> tuple[datetime, datetime]:
        if sample.date is None:
            raise ValueError(f"Patch sample {sample.sample_id} has no target date")
        return (
            sample.date + timedelta(days=selection.start_offset_days),
            sample.date + timedelta(days=selection.end_offset_days + 1),
        )

    def _coverage_buckets(self, start: datetime, end: datetime) -> list[tuple[int, int]]:
        """Expand requests to reusable calendar-aligned windows without querying the future."""
        now_limit = datetime.now(timezone.utc) + timedelta(days=1)
        end = min(end, now_limit)
        if start >= end:
            return []
        result: list[tuple[int, int]] = []
        cursor = datetime(start.year, 1, 1, tzinfo=timezone.utc)
        while cursor < end:
            candidate_end = min(
                datetime(cursor.year + 1, 1, 1, tzinfo=timezone.utc),
                cursor + timedelta(days=self.config.query_window_days),
                now_limit,
            )
            result.append((_millis(cursor), _millis(candidate_end)))
            cursor = candidate_end
        return result

    def prepare(
        self,
        samples: Sequence[SampleRecord],
        *,
        grid: PatchGrid,
        selection: SceneSelection,
        workload_tag: str,
    ) -> None:
        with self._lock:
            self._stats = ResolverStats()
        self._plans.clear()
        self._grids.clear()
        self._tiles.clear()
        self._bboxes.clear()
        self._quality.clear()
        requested: dict[tuple[int, int], set[str]] = defaultdict(set)
        for sample in samples:
            computed = compute_grid(sample.geometry, grid)
            tiles, bbox = self._tile_locator(computed)
            self._grids[sample.sample_id] = computed
            self._tiles[sample.sample_id] = tiles
            self._bboxes[sample.sample_id] = bbox
            start, end = self._requested_interval(sample, selection)
            for bucket in self._coverage_buckets(start, end):
                requested[bucket].update(tiles)

        sync_groups: list[tuple[list[str], int, int]] = []
        now = datetime.now(timezone.utc)
        recent_boundary = now - timedelta(days=self.config.recent_horizon_days)
        refresh_after = now - timedelta(hours=self.config.recent_refresh_hours)
        for (start, end), tiles in requested.items():
            missing_tiles: list[str] = []
            for tile in sorted(tiles):
                freshness = None
                if self.config.mode == "refresh" or _datetime(end) >= recent_boundary:
                    freshness = now if self.config.mode == "refresh" else refresh_after
                missing = self.catalog.missing_intervals(
                    self.collection, tile, start, end, fetched_after=freshness
                )
                if missing:
                    missing_tiles.append(tile)
                    self._add_stats(catalog_misses=1)
                else:
                    self._add_stats(catalog_hits=1)
            if self.config.mode != "offline":
                for tile_chunk in _chunks(missing_tiles, self.config.max_tiles_per_query):
                    sync_groups.append((list(tile_chunk), start, end))

        if sync_groups:
            with ThreadPoolExecutor(max_workers=self.config.metadata_workers) as pool:
                futures = {
                    pool.submit(self._sync_group, tiles, start, end, workload_tag): (
                        tiles,
                        start,
                        end,
                    )
                    for tiles, start, end in sync_groups
                }
                for future in as_completed(futures):
                    future.result()

        lookup_started = time.monotonic()
        for sample in samples:
            start, end = self._requested_interval(sample, selection)
            candidates = self.catalog.query(
                collection=self.collection,
                tiles=self._tiles[sample.sample_id],
                start=start,
                end=end,
                max_cloud_percentage=self.config.metadata_cloud_max,
                bbox=self._bboxes[sample.sample_id],
            )
            candidates = self._rank(candidates, sample.date, selection.mode)
            if self.cloud_masking:
                retained: list[SceneRecord] = []
                grid_hash = self.grid_hash(sample)
                for candidate in candidates:
                    cached = self.catalog.quality(
                        candidate,
                        grid_hash,
                        self.config.qa_band,
                        self.config.qa_threshold,
                        self.config.min_clear_fraction,
                    )
                    self._quality[(sample.sample_id, candidate.asset_id)] = cached
                    if cached is not None:
                        self._add_stats(quality_cache_hits=1)
                    if cached is None or cached[1]:
                        retained.append(candidate)
                candidates = retained
            self._plans[sample.sample_id] = candidates
        self._add_stats(lookup_seconds=time.monotonic() - lookup_started)

    def _sync_group(
        self, tiles: Sequence[str], start_millis: int, end_millis: int, workload_tag: str
    ) -> None:
        started = time.monotonic()
        for attempt in range(self.config.metadata_retries + 1):
            self._add_stats(compute_features_calls=1)
            try:
                scenes = list(
                    self._metadata_fetcher(
                        tiles, _datetime(start_millis), _datetime(end_millis), workload_tag
                    )
                )
                break
            except Exception:
                if attempt >= self.config.metadata_retries:
                    self._add_stats(metadata_seconds=time.monotonic() - started)
                    raise
                delay = self.config.retry_base_seconds * (2**attempt)
                time.sleep(delay + random.random() * min(1.0, delay / 4.0))
        self.catalog.upsert_and_mark_coverage(
            scenes,
            collection=self.collection,
            tiles=tiles,
            start=start_millis,
            end=end_millis,
        )
        self._add_stats(
            metadata_rows=len(scenes),
            metadata_seconds=time.monotonic() - started,
        )

    @staticmethod
    def _rank(
        scenes: Iterable[SceneRecord], target: datetime | None, mode: str
    ) -> list[SceneRecord]:
        if target is None:
            return []
        target_millis = _millis(target)
        unique = {scene.asset_id: scene for scene in scenes}
        cloud = lambda scene: (
            scene.cloud_percentage if scene.cloud_percentage is not None else float("inf")
        )
        if mode == "closest":
            key = lambda scene: (
                abs(scene.acquired_millis - target_millis),
                cloud(scene),
                scene.asset_id,
            )
        elif mode == "latest":
            key = lambda scene: (-scene.acquired_millis, cloud(scene), scene.asset_id)
        else:
            key = lambda scene: (scene.acquired_millis, cloud(scene), scene.asset_id)
        return sorted(unique.values(), key=key)

    def _fetch_metadata(
        self, tiles: Sequence[str], start: datetime, end: datetime, workload_tag: str
    ) -> Sequence[SceneRecord]:
        collection = (
            self.ee.ImageCollection(self.collection)
            .filter(self.ee.Filter.inList("MGRS_TILE", list(tiles)))
            .filterDate(start.isoformat(), end.isoformat())
        )

        def metadata(raw: Any) -> Any:
            image = self.ee.Image(raw)
            ring = self.ee.List(image.geometry().bounds(1).coordinates().get(0))
            lower = self.ee.List(ring.get(0))
            upper = self.ee.List(ring.get(2))
            return self.ee.Feature(
                None,
                {
                    "asset_id": image.id(),
                    "scene_id": image.get("system:index"),
                    "scene_time": image.get("system:time_start"),
                    "mgrs_tile": image.get("MGRS_TILE"),
                    "cloud_percentage": image.get("CLOUDY_PIXEL_PERCENTAGE"),
                    "product_id": image.get("PRODUCT_ID"),
                    "minx": lower.get(0),
                    "miny": lower.get(1),
                    "maxx": upper.get(0),
                    "maxy": upper.get(1),
                },
            )

        raw = self.ee.data.computeFeatures(
            {"expression": collection.map(metadata), "workloadTag": workload_tag}
        )
        scenes = []
        for feature in _features(raw):
            properties = feature.get("properties", {})
            if not properties.get("asset_id") or properties.get("scene_time") is None:
                continue
            scenes.append(
                SceneRecord(
                    collection=self.collection,
                    asset_id=str(properties["asset_id"]),
                    scene_id=str(properties.get("scene_id") or ""),
                    acquired_millis=int(properties["scene_time"]),
                    mgrs_tile=str(properties.get("mgrs_tile") or ""),
                    cloud_percentage=(
                        float(properties["cloud_percentage"])
                        if properties.get("cloud_percentage") is not None
                        else None
                    ),
                    bbox=(
                        float(properties["minx"]),
                        float(properties["miny"]),
                        float(properties["maxx"]),
                        float(properties["maxy"]),
                    ),
                    properties={"PRODUCT_ID": properties.get("product_id")},
                )
            )
        return scenes

    def candidates(self, sample: SampleRecord) -> list[SceneRecord]:
        return list(self._plans.get(sample.sample_id, ()))

    def order_samples(self, samples: Sequence[SampleRecord]) -> list[SampleRecord]:
        if not self.config.group_downloads:
            return list(samples)

        def key(sample: SampleRecord) -> tuple[str, str, str]:
            candidates = self._plans.get(sample.sample_id, ())
            tile = min(self._tiles.get(sample.sample_id, {""}))
            month = candidates[0].acquired_at.strftime("%Y-%m") if candidates else "9999-99"
            return tile, month, sample.sample_id

        return sorted(samples, key=key)

    def grid_hash(self, sample: SampleRecord) -> str:
        grid = self._grids[sample.sample_id]
        payload = json.dumps(grid.to_ee(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]

    def cached_quality(self, sample: SampleRecord, scene: SceneRecord) -> tuple[float, bool] | None:
        return self._quality.get((sample.sample_id, scene.asset_id))

    def record_quality(
        self, sample: SampleRecord, scene: SceneRecord, clear_fraction: float, accepted: bool
    ) -> None:
        self.catalog.record_quality(
            scene,
            self.grid_hash(sample),
            self.config.qa_band,
            self.config.qa_threshold,
            self.config.min_clear_fraction,
            clear_fraction,
            accepted,
        )
        self._quality[(sample.sample_id, scene.asset_id)] = (clear_fraction, accepted)
        if not accepted:
            self._add_stats(quality_rejections=1)

    def apply_inline_quality(self, image: Any, *, include_band: bool) -> tuple[Any, Any | None]:
        """Apply Cloud Score+ to an already preprocessed S2 image without an aggregation."""
        linked = image.linkCollection(
            self.ee.ImageCollection(CLOUD_SCORE_COLLECTION), [self.config.qa_band]
        )
        clear = linked.select(self.config.qa_band).gte(self.config.qa_threshold)
        masked = image.updateMask(clear)
        quality = clear.unmask(0).uint8().rename(INLINE_CLEAR_BAND) if include_band else None
        return masked, quality

    def quality_image(self, image: Any) -> Any:
        """Return only the one-byte Cloud Score+ clear mask for a cheap QA probe."""
        linked = image.linkCollection(
            self.ee.ImageCollection(CLOUD_SCORE_COLLECTION), [self.config.qa_band]
        )
        return (
            linked.select(self.config.qa_band)
            .gte(self.config.qa_threshold)
            .unmask(0)
            .uint8()
            .rename(INLINE_CLEAR_BAND)
        )


__all__ = [
    "CLOUD_SCORE_COLLECTION",
    "INLINE_CLEAR_BAND",
    "CatalogResolverConfig",
    "MGRSTileLocator",
    "ResolverStats",
    "S2CatalogResolver",
]
