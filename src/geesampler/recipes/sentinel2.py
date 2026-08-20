from __future__ import annotations

from typing import Any

from ..grid import compute_grid
from ..models import DEFAULT_PATCH_GRID, PatchGrid, SampleRecord
from ..resolver import INLINE_CLEAR_BAND

S2_BANDS = ("B2", "B3", "B4", "B8", "B11", "B12")
S2_COLLECTION = "COPERNICUS/S2_SR_HARMONIZED"
CLOUD_SCORE_COLLECTION = "GOOGLE/CLOUD_SCORE_PLUS/V1/S2_HARMONIZED"


def patch_geometry(sample: SampleRecord, grid: PatchGrid = DEFAULT_PATCH_GRID) -> Any:
    import ee

    computed = compute_grid(sample.geometry, grid)
    width = computed.width * computed.scale_x
    height = computed.height * abs(computed.scale_y)
    return ee.Geometry.Rectangle(
        [
            computed.translate_x,
            computed.translate_y - height,
            computed.translate_x + width,
            computed.translate_y,
        ],
        proj=computed.crs,
        geodesic=False,
    )


def sentinel2_collection(
    sample: SampleRecord,
    *,
    grid: PatchGrid = DEFAULT_PATCH_GRID,
    cloud_score_threshold: float = 0.60,
    min_clear_fraction: float = 0.80,
    server_quality_filter: bool = True,
) -> Any:
    """S2 SR linked with Cloud Score+, with optional server-side patch filtering."""
    import ee

    footprint = patch_geometry(sample, grid)
    source = ee.ImageCollection(S2_COLLECTION).filterBounds(footprint)
    if not server_quality_filter:
        return source
    cloud_score = ee.ImageCollection(CLOUD_SCORE_COLLECTION).filterBounds(footprint)
    linked = source.linkCollection(cloud_score, ["cs_cdf"])

    def score(image):
        clear = image.select("cs_cdf").gte(cloud_score_threshold)
        result = image.updateMask(clear).addBands(clear.unmask(0).uint8().rename(INLINE_CLEAR_BAND))
        fraction = clear.reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=footprint,
            scale=grid.scale,
            maxPixels=grid.size * grid.size * 2,
        ).get("cs_cdf")
        return result.set("_geesampler_clear_fraction", fraction)

    return linked.map(score).filter(ee.Filter.gte("_geesampler_clear_fraction", min_clear_fraction))


def sentinel2_catalog_collection(
    sample: SampleRecord,
    *,
    grid: PatchGrid = DEFAULT_PATCH_GRID,
    cloud_score_threshold: float = 0.60,
) -> Any:
    """S2 preprocessing for catalog resolution and inline patch-quality validation."""
    return sentinel2_collection(
        sample,
        grid=grid,
        cloud_score_threshold=cloud_score_threshold,
        server_quality_filter=False,
    )


def sentinel2_point_timeseries(
    sample: SampleRecord,
    *,
    days_before: int = 0,
    days_after: int = 365,
    cloud_score_threshold: float = 0.60,
) -> Any:
    """S2 observations whose Cloud Score+ value is clear at the sample point."""
    import ee

    if sample.date is None:
        raise ValueError(f"Point sample {sample.sample_id} has no anchor date")
    geometry = ee.Geometry(sample.geometry)
    start = ee.Date(sample.date.isoformat()).advance(-days_before, "day")
    end = ee.Date(sample.date.isoformat()).advance(days_after, "day")
    source = ee.ImageCollection(S2_COLLECTION).filterBounds(geometry).filterDate(start, end)
    cloud_score = ee.ImageCollection(CLOUD_SCORE_COLLECTION).filterBounds(geometry)
    linked = source.linkCollection(cloud_score, ["cs_cdf"])

    def score(image):
        value = (
            image.select("cs_cdf")
            .reduceRegion(reducer=ee.Reducer.first(), geometry=geometry, scale=10)
            .get("cs_cdf")
        )
        return image.set("_geesampler_point_cloud_score", value)

    return linked.map(score).filter(
        ee.Filter.gte("_geesampler_point_cloud_score", cloud_score_threshold)
    )


def polygon_mask(sample: SampleRecord) -> Any:
    import ee

    return ee.Image(0).byte().paint(ee.Geometry(sample.geometry), 1).rename("sample_mask")
