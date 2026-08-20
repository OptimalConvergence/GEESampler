# GEESampler

GEESampler is a Python package for concurrent, resumable extraction of point time
series and georeferenced image-patch sequences from Google Earth Engine (GEE). It
keeps Earth Engine preprocessing in normal Python callbacks while standardizing
sample inputs, exact pixel grids, downloads, manifests, progress metrics, and
workload-tagged EECU monitoring.

The default patch is **336×336 pixels at 10 m**. Patch size, scale, bands,
temporal selection, endpoint, and concurrency remain configurable.

## Installation

```bash
conda env create -f environment.yml
conda activate geesampler
```

Or install the package and selected extras:

```bash
python -m pip install -e '.[all,dev]'
```

Copy `.env.example` into your own shell or secret manager. Never place a service
account JSON file in this repository.

## Authentication and network

GEESampler accepts Earth Engine Application Default Credentials or a service
account:

```yaml
auth:
  project: ${GEE_PROJECT}
  service_account: ${GEE_SERVICE_ACCOUNT}
  key_file: ${GEE_KEY_FILE}
```

The package does not launch or reconfigure VPN software. It honors the standard
`HTTP_PROXY` and `HTTPS_PROXY` variables; on the development machine the existing
Mihomo service is available at `http://127.0.0.1:7890`.

## Python API

```python
from functools import partial

from geesampler import FileSampleSource, PatchGrid, Sampler, SceneSelection
from geesampler.recipes.sentinel2 import S2_BANDS, sentinel2_collection

sampler = Sampler.from_yaml("examples/configs/prepared_points.yaml")
samples = FileSampleSource("examples/data/points.csv").records()
grid = PatchGrid(size=336, scale=10)

summary = sampler.download_patch_series(
    samples,
    partial(sentinel2_collection, grid=grid),
    bands=S2_BANDS,
    grid=grid,
    selection=SceneSelection("latest", 0, 90, max_scenes=2),
    scenario="prepared-s2",
)
print(summary.bandwidth_mib_per_second)
```

The callback receives one normalized `SampleRecord` and returns an
`ee.ImageCollection`. This is the extension point for arbitrary server-side
preprocessing. `download_point_series` uses the same callback interface and
returns one CSV per point.

Prepared inputs can be CSV, GeoJSON, GeoPackage, Shapefile, or GeoParquet.
`EESampleSource` accepts a server-side `ee.FeatureCollection`. Both normalize to:

```python
SampleRecord(sample_id, geometry, date, properties)
```

The YAML equivalent is runnable with:

```bash
geesampler run examples/configs/prepared_points.yaml
geesampler run examples/configs/point_timeseries.yaml
```

## Outputs and resume

Each run directory contains:

- `images/`, `masks/`, or `timeseries/` data;
- `manifest.csv` with target/acquisition dates, geometry, labels, and provenance;
- `ledger.sqlite` for safe resume;
- `metrics.jsonl` with per-file latency and payload bytes;
- `eecu.jsonl` with Cloud Monitoring snapshots when available;
- `summary.json` with throughput and final status.

Files are written to unique partial paths and atomically renamed. A repeated run
with the same `run_id` skips outputs recorded as successful.

## EECU progress and guards

Every `computePixels` and `computeFeatures` request carries a unique run-level
workload tag. If the caller can read Cloud Monitoring, GEESampler polls:

- `earthengine.googleapis.com/project/cpu/usage_time` (completed EECU-seconds);
- `earthengine.googleapis.com/project/cpu/in_progress_usage_time` (in-progress
  EECU-seconds).

The monitoring identity needs permission to read project time series (for
example, the Cloud Monitoring Viewer role). When no separate
`GOOGLE_APPLICATION_CREDENTIALS` is set, service-account configurations reuse
their GEE key for monitoring.

Example guard:

```yaml
run:
  monitoring:
    enabled: true
    required: false
    poll_seconds: 30
    warning_eecu_hours: 0.5
    hard_eecu_hours: 1.0
```

At the hard threshold the scheduler stops submitting new samples, lets current
requests finish, and checkpoints the run. The guard uses the larger of completed
and in-progress reported usage so active work is considered without adding two
potentially overlapping metrics. Monetary estimates use completed usage only and
require an author-supplied `price_per_eecu_hour`.

Cloud Monitoring metrics are preview and delayed, and completed usage excludes
failed requests, so the console deliberately labels them **reported EECU** rather
than exact per-sample billing. If monitoring is unavailable and `required` is
false, downloads continue with a warning. Progress messages show completed/total,
elapsed time, ETA, cumulative samples/s and MiB/s, and both EECU metrics.

References: [Earth Engine monitoring](https://developers.google.com/earth-engine/guides/monitoring_usage),
[computation overview](https://developers.google.com/earth-engine/guides/computation_overview),
and [cost controls](https://developers.google.com/earth-engine/guides/cost_controls).

## Included experiments

- `examples/mtbs_s2.py`: 2018–2024 MTBS events, closest qualifying scenes before
  ignition and after the post-fire perimeter image date, event masks, and
  deterministic 80–120 km land negatives. MTBS does not expose a fire-contained
  date, so the date parsed from `Post_ID` is explicitly treated as the end proxy.
  MTBS currently ends on 2024-12-31 in the [GEE catalog](https://developers.google.com/earth-engine/datasets/catalog/USFS_GTAC_MTBS_burned_area_boundaries_v1).
- `examples/gedi_s2.py`: footprint-level GEDI04_A vector tables resolved through
  the official table index, filtered by quality, degradation, relative error,
  positive AGBD, and terrain slope, then paired with the closest clear S2 scene.
- `examples/mining_s2.py`: prepared Global-scale Mining Polygons v2 GeoParquet,
  one deterministic 0.1–8 km² polygon per country, aligned mask, and 2019 S2 scene.
  Cite [Maus et al. (2022)](https://doi.org/10.1594/PANGAEA.942325) and retain its
  CC-BY-SA-4.0 terms.

Cache and validate the authoritative IIASA GeoParquet before the mining trial:

```bash
geesampler cache-mining \
  "${GEESAMPLER_DATA_ROOT}/cache/global_mining_polygons_v2.parquet"
```

S2 quality uses Cloud Score+ `cs_cdf >= 0.60` and requires at least 80% clear
pixels over the requested patch. Default bands are B2, B3, B4, B8, B11, and B12.

## Validation and benchmarking

Create an RGB/mask figure:

```bash
geesampler validate image.tif preview.png --mask mask.tif
```

`geesampler.benchmark.benchmark_patch_downloads` runs the same samples across
standard/high-volume endpoints, 128/256/336/512 patch sizes, and 4/8/16 workers.
It writes raw and aggregated CSV files plus a static comparison of payload
bandwidth and reported EECU/sample. Payload bandwidth is downloaded bytes divided
by wall time; it is intentionally not whole-machine interface traffic.

Run the included two-repetition trial and refresh delayed EECU values afterward:

```bash
python examples/benchmark_s2.py
geesampler refresh-benchmark-eecu \
  "${GEESAMPLER_DATA_ROOT}/benchmarks/s2-small-final/benchmark_runs.csv" \
  examples/configs/mining.yaml
```

See [the validated live experiment report](docs/EXPERIMENT_RESULTS.md) for the
small-run results and interpretation.

## Development

```bash
pytest
ruff check src tests examples
python -m build
```

Live tests require an authorized GEE project and are kept separate from the
offline unit suite.
