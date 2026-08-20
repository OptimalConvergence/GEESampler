from __future__ import annotations

import hashlib
import os
import uuid
from collections.abc import Callable
from pathlib import Path

import requests

from .recipes.mining import MINING_STAC


def cache_mining_polygons(
    destination: str | Path,
    *,
    stac_url: str = MINING_STAC,
    progress: Callable[[int], None] | None = None,
) -> tuple[Path, str]:
    """Download and validate the authoritative IIASA GeoParquet asset atomically."""
    destination = Path(destination).expanduser()
    destination.parent.mkdir(parents=True, exist_ok=True)
    metadata = requests.get(stac_url, timeout=60)
    metadata.raise_for_status()
    stac = metadata.json()
    assets = stac.get("assets", {})
    candidates = [
        asset
        for name, asset in assets.items()
        if "mining" in name.lower()
        and str(asset.get("href", "")).lower().endswith((".parquet", ".geoparquet"))
    ]
    if not candidates:
        candidates = [
            asset
            for asset in assets.values()
            if str(asset.get("href", "")).lower().endswith((".parquet", ".geoparquet"))
        ]
    if len(candidates) != 1:
        raise RuntimeError(f"Expected one mining GeoParquet STAC asset, found {len(candidates)}")
    temporary = destination.with_suffix(destination.suffix + f".{uuid.uuid4().hex}.partial")
    digest = hashlib.sha256()
    downloaded = 0
    try:
        with requests.get(candidates[0]["href"], stream=True, timeout=120) as response:
            response.raise_for_status()
            with temporary.open("wb") as stream:
                chunks = response.iter_content(chunk_size=1024 * 1024)
                for chunk in chunks:
                    if not chunk:
                        continue
                    stream.write(chunk)
                    digest.update(chunk)
                    downloaded += len(chunk)
                    if progress:
                        progress(downloaded)
        import geopandas as gpd
        from pyproj import CRS

        frame = gpd.read_parquet(temporary)
        required = {"fid", "ISO3_CODE", "AREA"}
        missing = required.difference(frame.columns)
        if missing:
            raise ValueError(f"Mining dataset is missing columns: {sorted(missing)}")
        if len(frame) != 44_929:
            raise ValueError(f"Expected 44,929 mining polygons, found {len(frame):,}")
        wgs84 = CRS.from_epsg(4326)
        if frame.crs is None or not CRS.from_user_input(frame.crs).equals(
            wgs84, ignore_axis_order=True
        ):
            raise ValueError(f"Expected WGS84 mining data, found {frame.crs}")
        os.replace(temporary, destination)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    return destination, digest.hexdigest()
