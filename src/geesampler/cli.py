from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any

from .benchmark import refresh_benchmark_eecu
from .cache import cache_mining_polygons
from .config import SamplerConfig, load_callable, patch_settings
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
    sampler = Sampler(config)
    payload = dict(config.raw)
    source = _source(sampler, payload)
    source_data = payload.get("source", {})
    records = source.records(source_data.get("limit"))
    download = payload.get("download", {})
    builder = load_callable(download["collection_builder"])
    bands = download["bands"]
    kind = download.get("kind", "patch")
    if kind == "point":
        summary = sampler.download_point_series(
            records,
            builder,
            bands=bands,
            scale=float(download.get("scale", 10)),
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
        sampler = Sampler.from_yaml(args.config)
        print(refresh_benchmark_eecu(args.csv, sampler.config.auth.project))
    else:
        path, checksum = cache_mining_polygons(
            args.output,
            progress=lambda size: logging.getLogger(__name__).info(
                "downloaded %.1f MiB", size / (1024**2)
            ),
        )
        print(json.dumps({"path": str(path), "sha256": checksum}, indent=2))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
