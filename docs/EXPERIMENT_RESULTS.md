# Validated live experiments

These smoke tests ran on 2026-08-20 through an existing local HTTP proxy against
the configured development Earth Engine project. They validate the workflows; they are not
general performance promises. All patch runs used 10 m pixels, six S2 bands, the
Cloud Score+ filter, and concurrent threads.

## Optimization baseline

A focused eight-sample 336×336 profile attributed 51.67 of 90.43 summed worker
seconds (57.1%) to scene selection and 38.32 seconds (42.4%) to pixel download
and output. Remote `computeFeatures` plus `computePixels` represented 98.8% of
worker time; local mask splitting, SQLite ledger work, and metrics logging were
negligible. This is the pre-catalog baseline for the new read-through metadata
resolver. New catalog benchmarks must report cold and warm catalog results
separately rather than comparing a cold first case with warm later cases.

After implementation, a one-sample live warm-catalog smoke test downloaded one
128×128 six-band S2 patch successfully with zero discovery `computeFeatures`
calls, a 100% catalog hit rate, 0.667 sample/s, and 0.089 MiB/s payload
throughput. This validates the resolver and inline Cloud Score path only; a
single non-concurrent sample is not a production performance estimate.

A four-point staged-benchmark smoke test then completed the cold catalog-worker
matrix, two screen cases, finalist selection, isolated warm-catalog copies, final
run, and both plots. The retained 256×256/four-thread case completed 4/4 samples
at 2.112 samples/s and 1.091 MiB/s with a 100% catalog hit rate. All points shared
one metadata query group, so its 1/2/4 metadata-worker timings are not useful for
choosing worker count; the documented 128×3 geographically diverse benchmark is
still required for tuning conclusions.

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

## Global 128-sample staged benchmark

The complete staged benchmark used the first 128 Global-scale Mining Polygons
v2 records, a 32-sample/two-repetition screen, and three fresh-catalog repeats
for each of four retained cases. Each final case therefore attempted 384
samples. The metadata catalog was warm but the patch-quality cache was empty at
the start of every repeat. EECU values below were refreshed by workload tag
after the runs and are completed EECU-seconds per accepted sample.

| Final case | Accepted / attempted | Samples/s | MiB/s | ComputePixels p95 (s) | EECU-s/accepted |
|---|---:|---:|---:|---:|---:|
| standard / 336 / 8 / hybrid | 303 / 384 | 1.445 | 1.477 | 2.952 | 0.987 |
| high-volume / 128 / 8 / hybrid | 309 / 384 | 2.036 | 0.305 | 1.451 | 0.232 |
| high-volume / 512 / 8 / hybrid | 297 / 384 | 1.111 | 2.613 | 3.980 | 2.120 |
| high-volume / 336 / 16 / metadata-only | 324 / 384 | 3.040 | 2.665 | 2.880 | 0.484 |

Every final case had a 100% catalog hit rate and made zero metadata
`computeFeatures` calls during pixel sampling. Hybrid runs rejected an average
of 37--38 cloudy candidates per repeat and tried the next locally ranked scene;
metadata-only mode intentionally skipped that patch-level check. Acceptance
counts therefore differ by patch size and cloud mode and should be considered
alongside speed.

The screen also isolated two scheduling effects. Grouping 336-pixel work by
space and time improved the 16-worker high-volume case from 0.956 to 1.094
samples/s and reduced `ComputePixels` p95 from 4.215 to 2.776 seconds. Raising
workers from 16 to 24 or 32 reduced throughput and raised p95 above 5.16 seconds;
the HTTP client reported its ten-connection pool was full. Eight workers remains
the conservative hybrid default. Use 128 pixels when samples/s and EECU
efficiency matter, 512 pixels when payload bandwidth is the objective, and 336
pixels as the balanced training default requested for this package.

Cold catalog discovery over 32 global polygons required seven batched metadata
queries and returned 5,638 scene rows for every worker setting:

| Metadata workers | Wall time (s) | Samples/s | EECU-s/input |
|---:|---:|---:|---:|
| 1 | 66.99 | 0.478 | 1.231 |
| 2 | 36.83 | 0.869 | 1.081 |
| 4 | 13.91 | 2.300 | 1.472 |

Four metadata workers are the fastest tested cold-sync setting, while two
workers remain a lower-pressure default. Cloud Monitoring has delayed sampling
windows, so per-tag EECU is suitable for operational comparison and guardrails,
not exact billing reconciliation.
