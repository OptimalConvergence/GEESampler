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
account JSON file in this repository. JSON keys are rejected unless their Unix
permissions are owner-only (`chmod 600 /secure/path/key.json`).

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

### Multiple credential profiles

Independent OS processes can run account-isolated Earth Engine clients while a
parent scheduler groups nearby samples by half-degree cell and month, balances
load, and retries transient worker failures on a healthy profile:

```yaml
accounts:
  - name: primary
    workers: 8
    auth:
      project: ${GEE_PROJECT}
      high_volume: true
  - name: secondary
    workers: 8
    auth:
      project: ${GEE_PROJECT_SECONDARY}
      service_account: ${GEE_SERVICE_ACCOUNT_SECONDARY}
      key_file: ${GEE_KEY_FILE_SECONDARY}
      high_volume: true

distributed:
  enabled: true
  max_inflight_per_project: 16
  failover_attempts: 1
```

See `examples/configs/distributed.example.yaml` for a runnable template. Profile
names—not account identifiers—are written to aggregate metrics. Credentials stay
outside run artifacts, error messages redact credential-shaped values, and each
process has its own authenticated client and HTTP connection pool.

This feature is for legitimate workload isolation and reliability. It does not
increase quota when profiles share a Cloud project: project quotas and EECU are
shared, so `max_inflight_per_project` is enforced across those profiles. Do not
use multiple accounts or projects to circumvent Earth Engine quotas or access
restrictions. Prefer short-lived impersonated credentials when that
authentication path becomes available; static JSON is supported for the current
local deployment only.

For read and compute sampling, each service identity needs project-level Earth
Engine Resource Viewer (`roles/earthengine.viewer`) and Service Usage Consumer
(`roles/serviceusage.serviceUsageConsumer`). Use Earth Engine Resource Writer
only if the workload also creates or modifies EE assets. Cloud Monitoring Viewer
is additionally needed for EECU telemetry. See Google's
[Earth Engine access-control guide](https://developers.google.com/earth-engine/guides/access_control).

## Python API

```python
from functools import partial

from geesampler import FileSampleSource, PatchGrid, Sampler, SceneSelection
from geesampler.recipes.sentinel2 import S2_BANDS, sentinel2_catalog_collection

sampler = Sampler.from_yaml("examples/configs/prepared_points.yaml")
samples = FileSampleSource("examples/data/points.csv").records()
grid = PatchGrid(size=336, scale=10)

summary = sampler.download_patch_series(
    samples,
    partial(sentinel2_catalog_collection, grid=grid),
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
- `profile.json` with p50/p95/p99 time by discovery, pixel, QA, and write step;
- `eecu.jsonl` with Cloud Monitoring snapshots when available;
- `summary.json` with throughput and final status.

Files are written to unique partial paths and atomically renamed. A repeated run
with the same `run_id` skips outputs recorded as successful.

## Incremental S2 metadata catalog

Enable the embedded SQLite/RTree catalog to avoid discovering the same S2 scenes
with `computeFeatures` for every sample:

```yaml
catalog:
  enabled: true
  path: ${GEESAMPLER_DATA_ROOT:-./geesampler-output}/catalog/sentinel2.sqlite
  mode: read_through
  metadata_workers: 2
  query_window_days: 366
  max_tiles_per_query: 8
  metadata_cloud_max: 20
  cloud:
    mode: hybrid_inline
    band: cs_cdf
    threshold: 0.60
    min_clear_fraction: 0.80
```

`read_through` groups missing metadata by MGRS tile and calendar-aligned time
window, upserts it transactionally, and performs later date/geometry searches
locally. Historical coverage remains cached; the most recent 30 days are checked
again after 24 hours. Use `offline` to forbid catalog network fills or `refresh`
for an explicit refresh.

```bash
geesampler catalog sync examples/configs/prepared_points.yaml
geesampler catalog stats examples/configs/prepared_points.yaml
geesampler catalog import-geelinker examples/configs/prepared_points.yaml \
  /path/to/S2GridInfos --start 2017-01-01 --end 2025-01-01
```

Import dates are required because the legacy JSON files do not record which
date interval was queried. The database stores only public scene metadata and
patch-quality results—never credentials, authorization headers, or service-account
contents.

The production default `hybrid_inline` policy first applies the scene-wide
metadata cloud ceiling, then downloads an internal uint8 Cloud Score+ clear band
with the requested pixels. `hybrid_probe` instead requests that one-byte QA band
first and downloads the full masked multispectral patch only after acceptance;
this is preferable when metadata admits many locally cloudy candidates.
`metadata_only` uses no Cloud Score and is the deliberately cheaper benchmark
baseline. Clear-fraction decisions are cached per grid, scene, and QA policy.
Install the `geo` extra for either Cloud Score validation path.

## EECU progress and guards

Every `computePixels` and `computeFeatures` request carries a unique run-level
workload tag. If the caller can read Cloud Monitoring, GEESampler polls:

- `earthengine.googleapis.com/project/cpu/usage_time` (completed EECU-seconds);
- `earthengine.googleapis.com/project/cpu/in_progress_usage_time` (in-progress
  EECU-seconds).

The monitoring identity needs permission to read project time series (for
example, the Cloud Monitoring Viewer role). Cloud Monitoring uses the process's
Application Default Credentials. This keeps the Earth Engine service-account key
isolated from telemetry; grant the monitoring identity read access separately if
EECU progress is required.

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

S2 quality uses a metadata cloud ceiling of 20%, then Cloud Score+
`cs_cdf >= 0.60`, and requires at least 80% clear pixels over the requested
patch. Default bands are B2, B3, B4, B8, B11, and B12.

## Validation and benchmarking

Create an RGB/mask figure:

```bash
geesampler validate image.tif preview.png --mask mask.tif
```

`geesampler.benchmark.staged_benchmark_patch_downloads` first screens the same
samples across standard/high-volume endpoints, 128/256/336/512/768/1024/1536
patch sizes, 4/8/16/24/32 download threads, and all three cloud policies.
It retains four complementary configurations, then runs 128 samples with three
repetitions per retained case. It writes raw and aggregated CSV files
plus a static comparison of samples/s, wire and retained-output MiB/s,
megapixels/s, reported EECU/sample, and pixel-request p95 latency. Cold catalog synchronization with 1/2/4 metadata
workers is written separately to `catalog_worker_benchmark.csv`; every timed
pixel case receives an identical warm metadata catalog and an empty patch-quality
cache. `benchmark_account_scaling` runs the fair 1×8, 1×16, and 2×8 profile
matrix. Benchmark scheduling stops after 100 reported EECU-hours by default.

Wire bandwidth is downloaded bytes divided by wall time; useful bandwidth counts
only retained output bytes. A larger patch often raises wire MiB/s by amortizing
request overhead, but it is not automatically more efficient: compare samples/s,
megapixels/s, useful bandwidth, tail latency, and EECU per success. The estimator
rejects cases above the 48 MiB uncompressed `computePixels` request limit. A live
six-band S2 request at 1792 was rejected by Earth Engine at 57,802,752 bytes;
1536 completed below the limit and is therefore the largest default case. The
estimate is intentionally conservative because preprocessing can promote pixel
types. These metrics measure GEESampler payloads rather than whole-machine
interface traffic.

Run the included tuning trial and refresh delayed EECU values afterward:

```bash
python examples/benchmark_s2.py
geesampler refresh-benchmark-eecu \
  "${GEESAMPLER_DATA_ROOT}/benchmarks/s2-small-final/final/benchmark_runs.csv" \
  examples/configs/mining.yaml
geesampler refresh-benchmark-eecu \
  "${GEESAMPLER_DATA_ROOT}/benchmarks/s2-small-final/catalog-workers/catalog_worker_benchmark.csv" \
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
