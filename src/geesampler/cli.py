from __future__ import annotations

import argparse
import json
import logging
from dataclasses import asdict
from datetime import datetime, timezone
from functools import partial
from pathlib import Path
from typing import Any

from .benchmark import refresh_benchmark_eecu
from .cache import cache_mining_polygons
from .catalog import S2SceneCatalog
from .config import SamplerConfig, load_callable, patch_settings
from .distributed import DistributedSampler
from .engine import make_workload_tag
from .models import parse_datetime
from .resolver import S2_COLLECTION
from .sampler import Sampler
from .sources import EESampleSource, FileSampleSource
from .visualize import plot_benchmark, plot_sample_pair


def _source(sampler: Sampler, payload: dict[str, Any]):
    data = payload.get("source", {})
    source_type = data.get("type", "file")
    if source_type == "file":
        return FileSampleSource(
            data["path"],
            id_column=data.get("id_column", "SampleID"),
            date_column=data.get("date_column", "Date"),
            lon_column=data.get("lon_column", "Lon"),
            lat_column=data.get("lat_column", "Lat"),
        )
    if source_type == "gee":
        collection = sampler.ee.FeatureCollection(data["asset"])
        return EESampleSource(
            collection,
            id_property=data.get("id_property", "SampleID"),
            date_property=data.get("date_property", "Date"),
        )
    raise ValueError(f"Unknown source type: {source_type}")


def run_config(path: str | Path) -> dict[str, Any]:
    config = SamplerConfig.from_yaml(path)
    payload = dict(config.raw)
    sampler = Sampler(
        config,
        initialize=(
            not config.distributed.enabled
            or payload.get("source", {}).get("type", "file") == "gee"
        ),
    )
    source = _source(sampler, payload)
    source_data = payload.get("source", {})
    records = source.records(source_data.get("limit"))
    download = payload.get("download", {})
    builder_path = download["collection_builder"]
    builder_kwargs = dict(download.get("builder_kwargs", {}))
    builder = partial(load_callable(builder_path), **builder_kwargs)
    bands = download["bands"]
    kind = download.get("kind", "patch")
    if config.distributed.enabled:
        distributed = DistributedSampler(config)
        if kind == "point":
            _, selection = patch_settings(payload)
            summary = distributed.download_point_series(
                records,
                builder_path,
                builder_kwargs=builder_kwargs,
                bands=bands,
                scale=float(download.get("scale", 10)),
                selection=selection,
                scenario=download.get("scenario", "points"),
                run_id=download.get("run_id"),
            )
        else:
            grid, selection = patch_settings(payload)
            summary = distributed.download_patch_series(
                records,
                builder_path,
                builder_kwargs=builder_kwargs,
                bands=bands,
                grid=grid,
                selection=selection,
                mask_builder=download.get("mask_builder"),
                scenario=download.get("scenario", "patches"),
                run_id=download.get("run_id"),
            )
        return summary.to_dict()
    if kind == "point":
        _, selection = patch_settings(payload)
        summary = sampler.download_point_series(
            records,
            builder,
            bands=bands,
            scale=float(download.get("scale", 10)),
            selection=selection,
            scenario=download.get("scenario", "points"),
            run_id=download.get("run_id"),
        )
    else:
        grid, selection = patch_settings(payload)
        mask_builder = (
            load_callable(download["mask_builder"]) if download.get("mask_builder") else None
        )
        summary = sampler.download_patch_series(
            records,
            builder,
            bands=bands,
            grid=grid,
            selection=selection,
            mask_builder=mask_builder,
            scenario=download.get("scenario", "patches"),
            run_id=download.get("run_id"),
        )
    return summary.to_dict()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="geesampler")
    parser.add_argument("--log-level", default="INFO")
    subparsers = parser.add_subparsers(dest="command", required=True)
    run = subparsers.add_parser("run", help="Run a YAML sampling configuration")
    run.add_argument("config")
    validate = subparsers.add_parser("validate", help="Plot an image and aligned mask")
    validate.add_argument("image")
    validate.add_argument("output")
    validate.add_argument("--mask")
    plot = subparsers.add_parser("plot-benchmark", help="Plot benchmark summary CSV")
    plot.add_argument("csv")
    plot.add_argument("output")
    refresh = subparsers.add_parser(
        "refresh-benchmark-eecu", help="Refresh delayed EECU values in benchmark outputs"
    )
    refresh.add_argument("csv")
    refresh.add_argument("config")
    cache = subparsers.add_parser("cache-mining", help="Cache and validate Mining Polygons v2")
    cache.add_argument("output")
    catalog = subparsers.add_parser("catalog", help="Manage the local S2 metadata catalog")
    catalog_commands = catalog.add_subparsers(dest="catalog_command", required=True)
    catalog_sync = catalog_commands.add_parser("sync", help="Fill metadata for a run config")
    catalog_sync.add_argument("config")
    catalog_stats = catalog_commands.add_parser("stats", help="Show catalog coverage and size")
    catalog_stats.add_argument("config")
    catalog_import = catalog_commands.add_parser(
        "import-geelinker", help="Import existing per-MGRS GEELinker JSON metadata"
    )
    catalog_import.add_argument("config")
    catalog_import.add_argument("directory")
    catalog_import.add_argument("--start", required=True)
    catalog_import.add_argument("--end", required=True)
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    if args.command == "run":
        print(json.dumps(run_config(args.config), indent=2, sort_keys=True))
    elif args.command == "validate":
        print(plot_sample_pair(args.image, args.output, mask_path=args.mask))
    elif args.command == "plot-benchmark":
        print(plot_benchmark(args.csv, args.output))
    elif args.command == "refresh-benchmark-eecu":
        config = SamplerConfig.from_yaml(args.config)
        print(refresh_benchmark_eecu(args.csv, config.auth.project))
    elif args.command == "cache-mining":
        path, checksum = cache_mining_polygons(
            args.output,
            progress=lambda size: logging.getLogger(__name__).info(
                "downloaded %.1f MiB", size / (1024**2)
            ),
        )
        print(json.dumps({"path": str(path), "sha256": checksum}, indent=2))
    elif args.catalog_command == "stats":
        config = SamplerConfig.from_yaml(args.config)
        if config.catalog is None:
            raise ValueError("catalog.enabled must be true")
        print(json.dumps(asdict(S2SceneCatalog(config.catalog.path).stats()), indent=2))
    elif args.catalog_command == "import-geelinker":
        config = SamplerConfig.from_yaml(args.config)
        if config.catalog is None:
            raise ValueError("catalog.enabled must be true")
        start, end = parse_datetime(args.start), parse_datetime(args.end)
        if start is None or end is None:
            raise ValueError("catalog import dates cannot be empty")
        files, scenes = S2SceneCatalog(config.catalog.path).import_geelinker(
            args.directory, collection=S2_COLLECTION, start=start, end=end
        )
        print(json.dumps({"files": files, "scenes": scenes}, indent=2))
    else:
        config = SamplerConfig.from_yaml(args.config)
        sampler = Sampler(config)
        resolver = sampler.scene_resolver()
        if resolver is None:
            raise ValueError("catalog.enabled must be true")
        payload = dict(config.raw)
        records = list(_source(sampler, payload).records(payload.get("source", {}).get("limit")))
        grid, selection = patch_settings(payload)
        tag = make_workload_tag(
            config.run.workload_prefix,
            "catalog-sync",
            datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S"),
        )
        resolver.prepare(records, grid=grid, selection=selection, workload_tag=tag)
        print(json.dumps(asdict(resolver.stats()), indent=2))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
