from __future__ import annotations

import json
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Protocol

from .models import SampleRecord, parse_datetime


class SampleSource(Protocol):
    def records(self, limit: int | None = None) -> Iterable[SampleRecord]: ...


def _portable_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _portable_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_portable_value(item) for item in value]
    if hasattr(value, "item"):
        return _portable_value(value.item())
    return str(value)


def _record(
    properties: Mapping[str, Any],
    geometry: Mapping[str, Any],
    id_column: str,
    date_column: str | None,
) -> SampleRecord:
    sample_id = properties.get(id_column)
    if sample_id is None:
        raise ValueError(f"Missing sample id column/property: {id_column}")
    sample_date = parse_datetime(properties.get(date_column)) if date_column else None
    portable = {str(key): _portable_value(value) for key, value in properties.items()}
    return SampleRecord(str(sample_id), geometry, sample_date, portable)


@dataclass
class FileSampleSource:
    path: Path | str
    id_column: str = "SampleID"
    date_column: str | None = "Date"
    lon_column: str = "Lon"
    lat_column: str = "Lat"
    geometry_column: str = "geometry"

    def records(self, limit: int | None = None) -> Iterator[SampleRecord]:
        path = Path(self.path).expanduser()
        suffix = path.suffix.lower()
        if suffix == ".csv":
            yield from self._csv_records(path, limit)
            return
        if suffix in {".json", ".geojson"}:
            yield from self._geojson_records(path, limit)
            return
        if suffix not in {".gpkg", ".shp", ".parquet", ".geoparquet"}:
            raise ValueError(f"Unsupported sample file: {path}")
        try:
            import geopandas as gpd
            from shapely.geometry import mapping
        except ImportError as exc:  # pragma: no cover
            raise ImportError("This format requires the 'geo' extra") from exc
        frame = (
            gpd.read_parquet(path) if suffix in {".parquet", ".geoparquet"} else gpd.read_file(path)
        )
        if frame.crs and str(frame.crs).upper() != "EPSG:4326":
            frame = frame.to_crs(4326)
        if limit is not None:
            frame = frame.head(limit)
        for _, row in frame.iterrows():
            props = row.drop(labels=[frame.geometry.name]).to_dict()
            yield _record(props, mapping(row.geometry), self.id_column, self.date_column)

    def _csv_records(self, path: Path, limit: int | None) -> Iterator[SampleRecord]:
        import pandas as pd

        frame = pd.read_csv(path, nrows=limit)
        for _, row in frame.iterrows():
            props = row.to_dict()
            if self.geometry_column in frame.columns and props.get(self.geometry_column):
                raw = props[self.geometry_column]
                try:
                    geometry = json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    try:
                        from shapely import wkt
                        from shapely.geometry import mapping
                    except ImportError as exc:  # pragma: no cover
                        raise ImportError("WKT geometry requires the 'geo' extra") from exc
                    geometry = mapping(wkt.loads(str(raw)))
            else:
                geometry = {
                    "type": "Point",
                    "coordinates": [float(props[self.lon_column]), float(props[self.lat_column])],
                }
            yield _record(props, geometry, self.id_column, self.date_column)

    def _geojson_records(self, path: Path, limit: int | None) -> Iterator[SampleRecord]:
        payload = json.loads(path.read_text(encoding="utf-8"))
        features = (
            payload.get("features", []) if payload.get("type") == "FeatureCollection" else [payload]
        )
        if limit is not None:
            features = features[:limit]
        for feature in features:
            yield _record(
                feature.get("properties", {}),
                feature["geometry"],
                self.id_column,
                self.date_column,
            )


@dataclass
class EESampleSource:
    collection: Any
    id_property: str = "SampleID"
    date_property: str | None = "Date"
    workload_tag: str = "geesampler-source"

    def records(self, limit: int | None = None) -> Iterator[SampleRecord]:
        import ee

        collection = self.collection.limit(limit) if limit is not None else self.collection
        result = ee.data.computeFeatures(
            {"expression": collection, "workloadTag": self.workload_tag}
        )
        if hasattr(result, "iterrows"):
            for _, row in result.iterrows():
                raw = row.to_dict()
                geometry = raw.pop("geometry")
                if hasattr(geometry, "__geo_interface__"):
                    geometry = geometry.__geo_interface__
                yield _record(raw, geometry, self.id_property, self.date_property)
            return
        features = result.get("features", []) if isinstance(result, Mapping) else result
        for feature in features:
            yield _record(
                feature.get("properties", {}),
                feature["geometry"],
                self.id_property,
                self.date_property,
            )
