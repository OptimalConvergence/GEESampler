from __future__ import annotations

from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any

from .auth import configure_proxy, initialize_earth_engine
from .catalog import S2SceneCatalog
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
from .resolver import S2CatalogResolver


class Sampler:
    def __init__(self, config: SamplerConfig, *, initialize: bool = True):
        self.config = config
        configure_proxy(config.proxy_url)
        self.ee = (
            initialize_earth_engine(config.auth, pool_size=config.run.workers)
            if initialize
            else None
        )
        self._catalog_resolver: S2CatalogResolver | None = None

    @classmethod
    def from_yaml(cls, path: str | Path, *, initialize: bool = True) -> Sampler:
        return cls(SamplerConfig.from_yaml(path), initialize=initialize)

    def _engine(self) -> DownloadEngine:
        return DownloadEngine(
            self.config.auth.project,
            self.config.run,
            ee_module=self.ee,
        )

    def scene_resolver(self) -> S2CatalogResolver | None:
        if self.config.catalog is None:
            return None
        if self.ee is None:
            raise RuntimeError("Earth Engine must be initialized before creating a scene resolver")
        if self._catalog_resolver is None:
            self._catalog_resolver = S2CatalogResolver(
                S2SceneCatalog(self.config.catalog.path),
                ee_module=self.ee,
                config=self.config.catalog.resolver,
            )
        return self._catalog_resolver

    def download_patch_series(
        self,
        records: Iterable[SampleRecord],
        collection_builder: CollectionBuilder,
        *,
        bands: Sequence[str],
        grid: PatchGrid = DEFAULT_PATCH_GRID,
        selection: SceneSelection = DEFAULT_SCENE_SELECTION,
        mask_builder: MaskBuilder | None = None,
        scene_resolver: Any | None = None,
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
            scene_resolver=scene_resolver if scene_resolver is not None else self.scene_resolver(),
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
        selection: SceneSelection = DEFAULT_SCENE_SELECTION,
        scene_resolver: Any | None = None,
        scenario: str = "points",
        run_id: str | None = None,
    ) -> RunSummary:
        return self._engine().download_point_series(
            records,
            collection_builder,
            bands=bands,
            scale=scale,
            selection=selection,
            scene_resolver=scene_resolver if scene_resolver is not None else self.scene_resolver(),
            scenario=scenario,
            run_id=run_id,
        )
