from __future__ import annotations

from collections.abc import Sequence
from typing import Any

GEDI_COLLECTION = "LARSE/GEDI/GEDI04_A_002_MONTHLY"
GEDI_TABLE_INDEX = "LARSE/GEDI/GEDI04_A_002_INDEX"
DEM_COLLECTION = "COPERNICUS/DEM/GLO30_2024_1"
GEDI_EPOCH = "2018-01-01T00:00:00"


def quality_filtered_gedi(aoi: Any, start: str, end: str) -> Any:
    """GEDI04_A quality recipe ported from the existing GEELinker workflow."""
    import ee

    dem_source = ee.ImageCollection(DEM_COLLECTION).filterBounds(aoi).select("DEM")
    dem = dem_source.mosaic().setDefaultProjection(dem_source.first().projection())
    slope = ee.Terrain.slope(dem).rename("slope")

    def quality(image):
        relative_se = image.select("agbd_se").divide(image.select("agbd"))
        mask = (
            image.select("l4_quality_flag")
            .eq(1)
            .And(image.select("degrade_flag").eq(0))
            .And(image.select("agbd").gt(0))
            .And(relative_se.lte(0.5))
            .And(slope.lt(30))
        )
        return image.addBands(slope).updateMask(mask)

    return ee.ImageCollection(GEDI_COLLECTION).filterBounds(aoi).filterDate(start, end).map(quality)


def gedi_sample_features(
    aoi: Any,
    start: str,
    end: str,
    *,
    samples_per_month: int = 4,
    limit: int = 8,
    seed: int = 42,
) -> Any:
    """Create deterministic footprint samples with observation dates from delta_time."""
    import ee

    collection = quality_filtered_gedi(aoi, start, end)
    images = collection.toList(collection.size())

    def sample_image(raw):
        image = ee.Image(raw)
        sampled = (
            image.select(["agbd", "agbd_se", "delta_time", "slope"])
            .sample(region=aoi, scale=25, geometries=True, tileScale=4)
            .randomColumn("_geesampler_month_random", seed)
            .sort("_geesampler_month_random")
            .limit(samples_per_month)
        )

        def annotate(feature):
            delta = ee.Number(feature.get("delta_time"))
            observed = ee.Date(GEDI_EPOCH).advance(delta, "second")
            coords = feature.geometry().coordinates()
            sample_id = (
                ee.String("gedi-")
                .cat(ee.String(image.get("system:index")))
                .cat("-")
                .cat(delta.format("%.3f"))
                .cat("-")
                .cat(ee.Number(coords.get(0)).format("%.5f"))
                .cat("-")
                .cat(ee.Number(coords.get(1)).format("%.5f"))
            )
            return feature.set({"SampleID": sample_id, "Date": observed.format("YYYY-MM-dd")})

        return sampled.map(annotate)

    collections = images.map(sample_image)
    return (
        ee.FeatureCollection(collections)
        .flatten()
        .randomColumn("_geesampler_random", seed)
        .sort("_geesampler_random")
        .limit(limit)
    )


def find_gedi_granules(
    aoi: Any,
    start: str,
    end: str,
    *,
    limit: int = 10,
    workload_tag: str = "geesampler-gedi-index",
) -> list[str]:
    """Resolve GEDI04_A vector tables that overlap a region and time interval."""
    import ee

    index = (
        ee.FeatureCollection(GEDI_TABLE_INDEX)
        .filterBounds(aoi)
        .filter(ee.Filter.gte("time_end", start))
        .filter(ee.Filter.lte("time_start", end))
        .sort("time_start")
        .limit(limit)
    )
    from ..sources import EESampleSource

    source = EESampleSource(
        index.select(["table_id"]),
        id_property="table_id",
        date_property=None,
        workload_tag=workload_tag,
    )
    return [record.sample_id for record in source.records()]


def gedi_vector_sample_features(
    asset_ids: str | Sequence[str],
    *,
    aoi: Any | None = None,
    limit: int = 8,
    seed: int = 42,
    max_relative_error: float = 0.5,
    max_slope_degrees: float = 30,
    candidate_multiplier: int = 8,
) -> Any:
    """Create deterministic, strictly filtered samples from GEDI04_A vector tables.

    A bounded candidate set is evaluated against terrain slope so a small trial does
    not need to run a DEM reduction over every footprint in a large orbit table.
    """
    import ee

    if isinstance(asset_ids, str):
        asset_ids = [asset_ids]
    if not asset_ids:
        return ee.FeatureCollection([])

    collection = ee.FeatureCollection(asset_ids[0])
    for asset_id in asset_ids[1:]:
        collection = collection.merge(ee.FeatureCollection(asset_id))
    if aoi is not None:
        collection = collection.filterBounds(aoi)

    def relative_error(feature):
        feature = ee.Feature(feature)
        error = ee.Number(feature.get("agbd_se")).divide(ee.Number(feature.get("agbd")))
        return feature.set("_geesampler_relative_error", error)

    candidate_count = max(limit * candidate_multiplier, limit)
    candidates = (
        collection.filter(ee.Filter.eq("l4_quality_flag", 1))
        .filter(ee.Filter.eq("degrade_flag", 0))
        .filter(ee.Filter.gt("agbd", 0))
        .map(relative_error)
        .filter(ee.Filter.lte("_geesampler_relative_error", max_relative_error))
        .randomColumn("_geesampler_random", seed)
        .sort("_geesampler_random")
        .limit(candidate_count)
    )
    dem_source = ee.ImageCollection(DEM_COLLECTION).select("DEM")
    dem = dem_source.mosaic().setDefaultProjection(dem_source.first().projection())
    slope = ee.Terrain.slope(dem).rename("slope")

    def annotate(feature):
        feature = ee.Feature(feature)
        terrain_slope = slope.reduceRegion(
            reducer=ee.Reducer.first(),
            geometry=feature.geometry(),
            scale=30,
            bestEffort=True,
            maxPixels=1000,
        ).get("slope")
        observed = ee.Date(GEDI_EPOCH).advance(ee.Number(feature.get("delta_time")), "second")
        return feature.set(
            {
                "SampleID": ee.String("gedi-").cat(ee.String(feature.get("shot_number"))),
                "Date": observed.format("YYYY-MM-dd'T'HH:mm:ss"),
                "slope": terrain_slope,
            }
        )

    filtered = (
        candidates.map(annotate)
        .filter(ee.Filter.notNull(["slope"]))
        .filter(ee.Filter.lt("slope", max_slope_degrees))
        .limit(limit)
    )
    return filtered.select(
        [
            "SampleID",
            "Date",
            "shot_number",
            "agbd",
            "agbd_se",
            "l4_quality_flag",
            "degrade_flag",
            "sensitivity",
            "slope",
        ]
    )
