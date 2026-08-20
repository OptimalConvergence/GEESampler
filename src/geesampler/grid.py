from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from functools import lru_cache
from typing import Any

from .models import Geometry, PatchGrid


def representative_lon_lat(geometry: Geometry) -> tuple[float, float]:
    if geometry["type"] == "Point":
        lon, lat = geometry["coordinates"][:2]
        return float(lon), float(lat)
    try:
        from shapely.geometry import shape
    except ImportError as exc:  # pragma: no cover - dependency error is explicit
        raise ImportError("Polygon inputs require the 'geo' extra (shapely)") from exc
    point = shape(geometry).representative_point()
    return float(point.x), float(point.y)


def utm_epsg(lon: float, lat: float) -> str:
    zone = min(60, max(1, int((lon + 180.0) // 6.0) + 1))
    return f"EPSG:{32600 + zone if lat >= 0 else 32700 + zone}"


@lru_cache(maxsize=128)
def _transformer(epsg: str):
    try:
        from pyproj import Transformer
    except ImportError as exc:  # pragma: no cover
        raise ImportError("Patch downloads require pyproj") from exc
    return Transformer.from_crs("EPSG:4326", epsg, always_xy=True)


@dataclass(frozen=True)
class ComputedGrid:
    crs: str
    width: int
    height: int
    scale_x: float
    scale_y: float
    translate_x: float
    translate_y: float

    def to_ee(self) -> dict[str, Any]:
        return {
            "dimensions": {"width": self.width, "height": self.height},
            "affineTransform": {
                "scaleX": self.scale_x,
                "shearX": 0,
                "translateX": self.translate_x,
                "shearY": 0,
                "scaleY": self.scale_y,
                "translateY": self.translate_y,
            },
            "crsCode": self.crs,
        }


def compute_grid(geometry: Mapping[str, Any], spec: PatchGrid) -> ComputedGrid:
    lon, lat = representative_lon_lat(geometry)
    crs = utm_epsg(lon, lat)
    center_x, center_y = _transformer(crs).transform(lon, lat)
    width_m = spec.size * spec.scale
    return ComputedGrid(
        crs=crs,
        width=spec.size,
        height=spec.size,
        scale_x=spec.scale,
        scale_y=-spec.scale,
        translate_x=center_x - width_m / 2.0,
        translate_y=center_y + width_m / 2.0,
    )
