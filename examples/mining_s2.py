"""Prepared-file Global-scale Mining Polygons v2 experiment."""

import os
from functools import partial
from pathlib import Path

from geesampler import PatchGrid, Sampler, SceneSelection
from geesampler.recipes.mining import mining_records
from geesampler.recipes.sentinel2 import (
    S2_BANDS,
    polygon_mask,
    sentinel2_catalog_collection,
)


def main(config_path: str = "examples/configs/mining.yaml") -> None:
    sampler = Sampler.from_yaml(config_path)
    data_root = Path(os.environ.get("GEESAMPLER_DATA_ROOT", "./geesampler-output"))
    polygons = data_root / "cache" / "global_mining_polygons_v2.parquet"
    grid = PatchGrid(336, 10)
    sampler.download_patch_series(
        mining_records(polygons, limit=8),
        partial(sentinel2_catalog_collection, grid=grid),
        bands=S2_BANDS,
        grid=grid,
        selection=SceneSelection("closest", -90, 90, 1),
        mask_builder=polygon_mask,
        scenario="mining-s2",
    )


if __name__ == "__main__":
    main()
