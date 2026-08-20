# Validated live experiments

These smoke tests ran on 2026-08-20 through an existing local HTTP proxy against
the configured development Earth Engine project. They validate the workflows; they are not
general performance promises. All patch runs used 10 m pixels, six S2 bands, the
Cloud Score+ filter, and concurrent threads.

## Scenario results

| Scenario | Input | Result | Payload bandwidth |
|---|---|---:|---:|
| Prepared S2 | local CSV points | 2 samples, 4 scenes | 0.56 MiB/s |
| GEDI/S2 | GEE vector granule index | 8/8 pairs | 0.69 MiB/s |
| MTBS pre-fire | GEE MTBS polygons | 4/4 pairs | 0.13 MiB/s |
| MTBS post-fire | GEE MTBS polygons | 4/4 pairs | 0.25 MiB/s |
| MTBS negative pre/post | generated 80–120 km controls | 6/8 pairs | 0.08–0.56 MiB/s |
| Mining/S2 | prepared IIASA GeoParquet | 8/8 pairs | 0.67 MiB/s |
| Point time series | local CSV points | 2/2 CSVs; 67 and 55 observations | 0.40 samples/s |

The two missing negative pairs are expected quality-filter outcomes: no qualifying
S2 scene occurred within their strict ±7-day windows. GEDI sampling preserved
footprint `shot_number`, AGBD, uncertainty, acquisition time, and slope. Image and
mask rasters were checked as aligned 336×336 grids; masks contained only 0/1.

The cached mining source contained 44,929 WGS84 polygons and had SHA-256
`86d7ce23024d3250fd1b19fcc76e799ebb5f4a3085d16b8143f6971fd63d814c`.

## Two-repetition patch benchmark

Each row is the mean of two runs over the same eight mining samples (16 successful
samples and no failures per row). Payload MiB/s is bytes divided by wall time.
EECU values were refreshed after Cloud Monitoring propagation and are completed,
workload-tagged EECU-seconds per successful sample.

| Endpoint / size / workers | MiB/s | Samples/s | EECU-s/sample |
|---|---:|---:|---:|
| standard / 336 / 8 | 0.976 | 1.113 | 1.324 |
| high-volume / 336 / 8 | 1.123 | 1.282 | 1.180 |
| high-volume / 128 / 8 | 0.284 | 2.180 | 0.751 |
| high-volume / 256 / 8 | 0.556 | 1.095 | 0.977 |
| high-volume / 512 / 8 | 3.090 | 1.556 | 1.711 |
| high-volume / 336 / 4 | 0.912 | 1.041 | 1.273 |
| high-volume / 336 / 16 | 1.391 | 1.588 | 1.197 |

The 512-pixel case has the highest payload bandwidth because larger responses
amortize request overhead; that does not make it the fastest sample rate or the
lowest-compute choice. In this small trial, 16 workers improved 336-pixel sample
rate over 4 and 8 workers. Run the benchmark on the target region and network
before selecting production defaults.
