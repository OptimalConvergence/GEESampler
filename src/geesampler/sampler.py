from __future__ import annotations

from collections.abc import Iterable, Sequence
from pathlib import Path

from .auth import configure_proxy, initialize_earth_engine
from .config import SamplerConfig
from .engine import DownloadEngine
from .models import (
    DEFAULT_PATCH_GRID,
    DEFAULT_SCENE_SELECTION,
    CollectionBuilder,
    MaskBuilder,
    PatchGrid,
    RunSummary,
    SampleRecord,
    SceneSelection,
)


class Sampler:
    def __init__(self, config: SamplerConfig, *, initialize: bool = True):
        self.config = config
        configure_proxy(config.proxy_url)
        self.ee = initialize_earth_engine(config.auth) if initialize else None

    @classmethod
    def from_yaml(cls, path: str | Path, *, initialize: bool = True) -> Sampler:
        return cls(SamplerConfig.from_yaml(path), initialize=initialize)

    def _engine(self) -> DownloadEngine:
        return DownloadEngine(
            self.config.auth.project,
            self.config.run,
            ee_module=self.ee,
        )

    def download_patch_series(
        self,
        records: Iterable[SampleRecord],
        collection_builder: CollectionBuilder,
        *,
        bands: Sequence[str],
        grid: PatchGrid = DEFAULT_PATCH_GRID,
        selection: SceneSelection = DEFAULT_SCENE_SELECTION,
        mask_builder: MaskBuilder | None = None,
        scenario: str = "patches",
        run_id: str | None = None,
    ) -> RunSummary:
        return self._engine().download_patch_series(
            records,
            collection_builder,
            bands=bands,
            grid=grid,
            selection=selection,
            mask_builder=mask_builder,
            scenario=scenario,
            run_id=run_id,
        )

    def download_point_series(
        self,
        records: Iterable[SampleRecord],
        collection_builder: CollectionBuilder,
        *,
        bands: Sequence[str],
        scale: float = 10,
        scenario: str = "points",
        run_id: str | None = None,
    ) -> RunSummary:
        return self._engine().download_point_series(
            records,
            collection_builder,
            bands=bands,
            scale=scale,
            scenario=scenario,
            run_id=run_id,
        )
