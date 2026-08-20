"""Small concurrent MTBS positive/negative S2 experiment."""

from functools import partial

from geesampler import EESampleSource, PatchGrid, Sampler, SceneSelection
from geesampler.recipes.mtbs import (
    mtbs_source,
    negative_candidates,
    negative_pairs,
    positive_pairs,
)
from geesampler.recipes.sentinel2 import S2_BANDS, polygon_mask, sentinel2_collection


def zero_mask(_sample):
    import ee

    return ee.Image(0).byte().rename("sample_mask")


def main(config_path: str = "examples/configs/mtbs.yaml") -> None:
    sampler = Sampler.from_yaml(config_path)
    grid = PatchGrid(336, 10)
    builder = partial(sentinel2_collection, grid=grid)
    events = list(mtbs_source(limit=4).records())
    positives = list(positive_pairs(events))
    pre = [sample for sample in positives if sample.properties["Phase"] == "pre"]
    post = [sample for sample in positives if sample.properties["Phase"] == "post"]
    sampler.download_patch_series(
        pre,
        builder,
        bands=S2_BANDS,
        grid=grid,
        selection=SceneSelection("closest", -90, -1, 1),
        mask_builder=polygon_mask,
        scenario="mtbs-positive-pre",
    )
    sampler.download_patch_series(
        post,
        builder,
        bands=S2_BANDS,
        grid=grid,
        selection=SceneSelection("closest", 1, 90, 1),
        mask_builder=polygon_mask,
        scenario="mtbs-positive-post",
    )

    candidates = negative_candidates(events)
    valid = list(
        EESampleSource(
            candidates,
            id_property="Event_ID",
            date_property=None,
            workload_tag="geesampler-mtbs-negative-source",
        ).records()
    )
    negatives = list(negative_pairs(events, valid))
    for phase in ("pre", "post"):
        samples = [item for item in negatives if item.properties["Phase"] == phase]
        sampler.download_patch_series(
            samples,
            builder,
            bands=S2_BANDS,
            grid=grid,
            selection=SceneSelection("closest", -7, 7, 1),
            mask_builder=zero_mask,
            scenario=f"mtbs-negative-{phase}",
        )


if __name__ == "__main__":
    main()
