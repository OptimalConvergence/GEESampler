from __future__ import annotations

import random
import re
from collections.abc import Iterable, Iterator, Sequence
from datetime import datetime, timezone
from typing import Any

from ..grid import representative_lon_lat
from ..models import SampleRecord
from ..sources import EESampleSource

MTBS_COLLECTION = "USFS/GTAC/MTBS/burned_area_boundaries/v1"
_DATE = re.compile(r"((?:19|20)\d{6})")


def mtbs_source(
    *, start: str = "2018-01-01", end: str = "2025-01-01", limit: int = 8
) -> EESampleSource:
    import ee

    collection = (
        ee.FeatureCollection(MTBS_COLLECTION)
        .filter(ee.Filter.gte("Ig_Date", ee.Date(start).millis()))
        .filter(ee.Filter.lt("Ig_Date", ee.Date(end).millis()))
        .sort("Ig_Date")
        .limit(limit)
    )
    return EESampleSource(
        collection,
        id_property="Event_ID",
        date_property="Ig_Date",
        workload_tag="geesampler-mtbs-source",
    )


def post_id_date(post_id: str) -> datetime:
    matches = _DATE.findall(str(post_id))
    if not matches:
        raise ValueError(f"Cannot derive end-date proxy from MTBS Post_ID: {post_id!r}")
    return datetime.strptime(matches[-1], "%Y%m%d").replace(tzinfo=timezone.utc)


def positive_pairs(events: Iterable[SampleRecord]) -> Iterator[SampleRecord]:
    for event in events:
        if event.date is None:
            continue
        event_id = str(event.properties.get("Event_ID", event.sample_id))
        common = dict(event.properties)
        common.update({"Event_ID": event_id, "GroupID": event_id, "Class": 1})
        yield SampleRecord(
            f"{event_id}-pre",
            event.geometry,
            event.date,
            {**common, "Phase": "pre", "Anchor": "Ig_Date"},
        )
        try:
            end_proxy = post_id_date(str(event.properties.get("Post_ID", "")))
        except ValueError:
            continue
        yield SampleRecord(
            f"{event_id}-post",
            event.geometry,
            end_proxy,
            {**common, "Phase": "post", "Anchor": "Post_ID_date"},
        )


def negative_candidates(
    events: Sequence[SampleRecord],
    *,
    candidates_per_event: int = 20,
    seed: int = 42,
    min_distance_km: float = 80,
    max_distance_km: float = 120,
) -> Any:
    """Batch-check deterministic distant candidates for land and MTBS non-intersection."""
    import ee
    from pyproj import Geod

    geod = Geod(ellps="WGS84")
    features = []
    for event in events:
        lon, lat = representative_lon_lat(event.geometry)
        event_seed = seed + sum(ord(char) for char in event.sample_id)
        generator = random.Random(event_seed)
        for index in range(candidates_per_event):
            bearing = generator.uniform(0, 360)
            distance_km = generator.uniform(min_distance_km, max_distance_km)
            candidate_lon, candidate_lat, _ = geod.fwd(lon, lat, bearing, distance_km * 1000)
            features.append(
                ee.Feature(
                    ee.Geometry.Point([candidate_lon, candidate_lat]),
                    {
                        "Event_ID": event.sample_id,
                        "candidate_index": index,
                        "distance_km": distance_km,
                        "bearing_deg": bearing,
                    },
                )
            )
    candidates = ee.FeatureCollection(features)
    fires = ee.FeatureCollection(MTBS_COLLECTION)
    land = ee.ImageCollection("ESA/WorldCover/v200").first().select("Map").neq(80)

    def validate(feature):
        footprint = feature.geometry().buffer(2400)
        no_fire = fires.filterBounds(footprint).size().eq(0)
        is_land = land.reduceRegion(ee.Reducer.first(), feature.geometry(), 10).get("Map")
        return feature.set({"no_mtbs": no_fire, "is_land": is_land})

    return (
        candidates.map(validate)
        .filter(ee.Filter.eq("no_mtbs", 1))
        .filter(ee.Filter.eq("is_land", 1))
    )


def negative_pairs(
    events: Sequence[SampleRecord], valid_candidates: Iterable[SampleRecord]
) -> Iterator[SampleRecord]:
    by_event: dict[str, SampleRecord] = {}
    for candidate in valid_candidates:
        by_event.setdefault(str(candidate.properties.get("Event_ID")), candidate)
    for positive in positive_pairs(events):
        event_id = str(positive.properties["Event_ID"])
        candidate = by_event.get(event_id)
        if not candidate:
            continue
        yield SampleRecord(
            f"{event_id}-negative-{positive.properties['Phase']}",
            candidate.geometry,
            positive.date,
            {
                **candidate.properties,
                "Event_ID": event_id,
                "GroupID": event_id,
                "Class": 0,
                "Phase": positive.properties["Phase"],
                "temporal_window_days": 7,
            },
        )
