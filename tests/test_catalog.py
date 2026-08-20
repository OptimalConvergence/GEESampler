import json
from datetime import datetime, timezone

from geesampler.catalog import S2SceneCatalog, SceneRecord
from geesampler.grid import compute_grid
from geesampler.models import PatchGrid, SampleRecord, SceneSelection
from geesampler.resolver import (
    INLINE_CLEAR_BAND,
    CatalogResolverConfig,
    MGRSTileLocator,
    S2CatalogResolver,
)

COLLECTION = "COPERNICUS/S2_SR_HARMONIZED"


def test_mgrs_locator_includes_adjacent_sentinel_zone_overlap():
    assert INLINE_CLEAR_BAND[0].isalnum()
    grid = compute_grid({"type": "Point", "coordinates": [-65.95, 10.25]}, PatchGrid(128, 10))
    tiles, _ = MGRSTileLocator()(grid)
    assert {"20PJS", "19PHM"}.issubset(tiles)


def _scene(asset: str, when: int, cloud: float = 5.0) -> SceneRecord:
    return SceneRecord(
        COLLECTION,
        asset,
        asset.rsplit("/", 1)[-1],
        when,
        "31TCJ",
        cloud,
        (-1.0, -1.0, 1.0, 1.0),
    )


def test_catalog_query_coverage_and_quality(tmp_path):
    catalog = S2SceneCatalog(tmp_path / "s2.sqlite")
    start = datetime(2020, 1, 1, tzinfo=timezone.utc)
    end = datetime(2021, 1, 1, tzinfo=timezone.utc)
    scene = _scene("collection/scene-a", 1_593_561_600_000)
    catalog.upsert_and_mark_coverage(
        [scene], collection=COLLECTION, tiles={"31TCJ"}, start=start, end=end
    )

    assert catalog.missing_intervals(COLLECTION, "31TCJ", start, end) == []
    found = catalog.query(
        collection=COLLECTION,
        tiles={"31TCJ"},
        start=start,
        end=end,
        max_cloud_percentage=20,
        bbox=(-0.5, -0.5, 0.5, 0.5),
    )
    assert [item.asset_id for item in found] == [scene.asset_id]
    assert (
        catalog.query(
            collection=COLLECTION,
            tiles={"31TCJ"},
            start=start,
            end=end,
            max_cloud_percentage=2,
        )
        == []
    )

    catalog.record_quality(scene, "grid", "cs_cdf", 0.6, 0.8, 0.9, True)
    assert catalog.quality(scene, "grid", "cs_cdf", 0.6, 0.8) == (0.9, True)
    stats = catalog.stats()
    assert stats.scenes == stats.tiles == stats.coverage_intervals == 1
    assert stats.patch_quality_rows == 1


def test_geelinker_import_is_incremental_and_bbox_optional(tmp_path):
    source = tmp_path / "json"
    source.mkdir()
    (source / "31TCJ.json").write_text(
        json.dumps(
            {
                "features": [
                    {
                        "id": "collection/scene-a",
                        "properties": {
                            "system:index": "scene-a",
                            "system:time_start": 1_593_561_600_000,
                            "MGRS_TILE": "31TCJ",
                            "CLOUDY_PIXEL_PERCENTAGE": 3,
                        },
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    catalog = S2SceneCatalog(tmp_path / "s2.sqlite")
    start = datetime(2020, 1, 1, tzinfo=timezone.utc)
    end = datetime(2021, 1, 1, tzinfo=timezone.utc)
    assert catalog.import_geelinker(source, collection=COLLECTION, start=start, end=end) == (1, 1)
    assert catalog.import_geelinker(source, collection=COLLECTION, start=start, end=end) == (0, 0)
    assert (
        len(
            catalog.query(
                collection=COLLECTION,
                tiles={"31TCJ"},
                start=start,
                end=end,
                max_cloud_percentage=20,
                bbox=(-0.5, -0.5, 0.5, 0.5),
            )
        )
        == 1
    )


def test_resolver_groups_cold_metadata_and_warm_run_uses_zero_remote_calls(tmp_path):
    catalog = S2SceneCatalog(tmp_path / "s2.sqlite")
    calls = []

    def locate(_grid):
        return {"31TCJ"}, (-1.0, -1.0, 1.0, 1.0)

    def fetch(tiles, start, end, tag):
        calls.append((tuple(tiles), start, end, tag))
        return [_scene("collection/scene-a", 1_593_561_600_000)]

    config = CatalogResolverConfig(
        mode="read_through",
        metadata_workers=2,
        recent_horizon_days=0,
        cloud_mode="metadata_only",
    )
    samples = [
        SampleRecord(
            f"sample-{index}",
            {"type": "Point", "coordinates": [0, 0]},
            datetime(2020, 7, 1 + index, tzinfo=timezone.utc),
        )
        for index in range(2)
    ]
    first = S2CatalogResolver(
        catalog,
        ee_module=object(),
        config=config,
        tile_locator=locate,
        metadata_fetcher=fetch,
    )
    first.prepare(samples, grid=PatchGrid(), selection=SceneSelection(), workload_tag="tag")
    assert len(calls) == 1
    assert first.stats().compute_features_calls == 1
    assert first.candidates(samples[0])[0].scene_id == "scene-a"

    second = S2CatalogResolver(
        catalog,
        ee_module=object(),
        config=config,
        tile_locator=locate,
        metadata_fetcher=fetch,
    )
    second.prepare(samples, grid=PatchGrid(), selection=SceneSelection(), workload_tag="tag")
    assert len(calls) == 1
    assert second.stats().compute_features_calls == 0
    assert second.stats().catalog_hit_rate == 1.0


def test_resolver_retries_metadata_without_marking_partial_coverage(tmp_path):
    catalog = S2SceneCatalog(tmp_path / "s2.sqlite")
    attempts = 0

    def fetch(_tiles, _start, _end, _tag):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ConnectionError("transient")
        return [_scene("collection/scene-a", 1_593_561_600_000)]

    resolver = S2CatalogResolver(
        catalog,
        ee_module=object(),
        config=CatalogResolverConfig(
            metadata_retries=1,
            retry_base_seconds=0,
            recent_horizon_days=0,
            cloud_mode="metadata_only",
        ),
        tile_locator=lambda _grid: ({"31TCJ"}, (-1, -1, 1, 1)),
        metadata_fetcher=fetch,
    )
    sample = SampleRecord(
        "sample",
        {"type": "Point", "coordinates": [0, 0]},
        datetime(2020, 7, 1, tzinfo=timezone.utc),
    )
    resolver.prepare([sample], grid=PatchGrid(), selection=SceneSelection(), workload_tag="tag")
    assert attempts == resolver.stats().compute_features_calls == 2
    assert resolver.candidates(sample)


def test_hybrid_probe_reuses_cached_quality_decision(tmp_path):
    catalog = S2SceneCatalog(tmp_path / "s2.sqlite")
    sample = SampleRecord(
        "sample",
        {"type": "Point", "coordinates": [0, 0]},
        datetime(2020, 7, 1, tzinfo=timezone.utc),
    )
    config = CatalogResolverConfig(
        recent_horizon_days=0,
        cloud_mode="hybrid_probe",
    )
    resolver = S2CatalogResolver(
        catalog,
        ee_module=object(),
        config=config,
        tile_locator=lambda _grid: ({"31TCJ"}, (-1, -1, 1, 1)),
        metadata_fetcher=lambda *_args: [_scene("collection/scene-a", 1_593_561_600_000)],
    )
    resolver.prepare([sample], grid=PatchGrid(), selection=SceneSelection(), workload_tag="tag")
    scene = resolver.candidates(sample)[0]
    catalog.record_quality(scene, resolver.grid_hash(sample), "cs_cdf", 0.6, 0.8, 0.2, False)

    cached = S2CatalogResolver(
        catalog,
        ee_module=object(),
        config=config,
        tile_locator=lambda _grid: ({"31TCJ"}, (-1, -1, 1, 1)),
        metadata_fetcher=lambda *_args: [],
    )
    cached.prepare([sample], grid=PatchGrid(), selection=SceneSelection(), workload_tag="tag")
    assert cached.candidates(sample) == []
    assert cached.stats().quality_cache_hits == 1
