"""Small concurrent S2/GEDI biomass-pair experiment."""

from functools import partial

import ee

from geesampler import EESampleSource, PatchGrid, Sampler, SceneSelection
from geesampler.recipes.gedi import find_gedi_granules, gedi_vector_sample_features
from geesampler.recipes.sentinel2 import S2_BANDS, sentinel2_collection


def main(config_path: str = "examples/configs/gedi.yaml") -> None:
    sampler = Sampler.from_yaml(config_path)
    # A forested segment of the catalog's June 2022 example orbit.
    trial_aoi = ee.Geometry.Rectangle([-66.3, 9.9, -65.8, 10.6])
    granules = find_gedi_granules(
        trial_aoi,
        "2022-06-01T00:00:00Z",
        "2022-06-10T00:00:00Z",
        limit=2,
    )
    if not granules:
        raise RuntimeError("No GEDI04_A vector granules overlap the trial query")
    footprints = gedi_vector_sample_features(granules, aoi=trial_aoi, limit=8)
    samples = EESampleSource(
        footprints,
        id_property="SampleID",
        date_property="Date",
        workload_tag="geesampler-gedi-source",
    ).records()
    grid = PatchGrid(336, 10)
    sampler.download_patch_series(
        samples,
        partial(sentinel2_collection, grid=grid),
        bands=S2_BANDS,
        grid=grid,
        selection=SceneSelection("closest", -90, 90, 1),
        scenario="gedi-s2",
    )


if __name__ == "__main__":
    main()
