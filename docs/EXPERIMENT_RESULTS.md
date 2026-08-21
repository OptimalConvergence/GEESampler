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

## v0.3 account, cloud, patch, and connection-pool benchmark

This follow-up ran on 2026-08-21 local time after adding per-profile processes,
HTTP pools sized to worker count, the Cloud Score probe path, and separate wire
versus retained-output metrics. All timed pixel cases used the high-volume
endpoint, a warm catalog with zero `computeFeatures` calls, six S2 bands, and one
repetition. Absolute throughput was lower than the prior run, so comparisons
below are within this run only.

The eight-sample metadata-only patch sweep produced:

| Patch | Accepted | Samples/s | Useful MiB/s | MP/s | p95 pixel time (s) | EECU-s/success |
|---:|---:|---:|---:|---:|---:|---:|
| 128 | 8/8 | 0.326 | 0.042 | 0.005 | 3.21 | 0.123 |
| 256 | 8/8 | 0.345 | 0.174 | 0.023 | 9.48 | 0.301 |
| 336 | 8/8 | 0.363 | 0.313 | 0.041 | 17.04 | 0.491 |
| 512 | 8/8 | 0.146 | 0.284 | 0.038 | 41.99 | 1.023 |
| 768 | 8/8 | 0.286 | 1.239 | 0.169 | 25.32 | 2.077 |
| 1024 | 8/8 | 0.397 | 3.029 | 0.416 | 9.30 | 3.087 |
| 1536 | 7/8 | 0.017 | 0.286 | 0.041 | 218.10 | 9.052 |
| 1792 | 0/8 | 0 | 0 | 0 | n/a | n/a |

Earth Engine rejected seven 1792 requests because their uncompressed
request was 57,802,752 bytes, above the 50,331,648-byte limit; the remaining
candidate did not resolve through the preprocessed collection. This live result supersedes the initial
uint16-only estimate and is why the package now uses a conservative promoted-
type estimate and stops the default sweep at 1536. Although 1024 delivered the
best successful wire and spatial throughput in this small run, it used about
6.3 times the EECU per success of 336. The 1536 tail makes it unsuitable as a
general default. Keep 336 as the balanced training default; use 768 or 1024 only
when larger spatial context materially benefits the model.

At 336 pixels, the cloud-policy comparison was:

| Policy | Samples/s | Wire MiB/s | Useful MiB/s | Wire efficiency | QA rejections | p95 (s) | EECU-s/success |
|---|---:|---:|---:|---:|---:|---:|---:|
| Metadata only | 0.363 | 0.313 | 0.313 | 100.0% | 0 | 17.04 | 0.491 |
| Hybrid probe | 0.537 | 0.463 | 0.462 | 99.8% | 4 | 7.40 | 0.880 |
| Hybrid inline | 0.198 | 0.206 | 0.172 | 83.2% | 4 | 10.92 | 0.978 |

Metadata-only remains a speed baseline, not a production-quality equivalent.
Both Cloud Score modes rejected the same four candidates. The probe avoided
full multispectral transfers for those candidates and outperformed inline QA in
this run, so `hybrid_probe` is the preferred production experiment when local
cloud rejection is common. Retain `hybrid_inline` when a single request is more
important than minimizing rejected bytes.

The saturated 32-sample metadata-only worker sweep used the same inputs in every
case; each case accepted the same 27 samples and hit the same five unresolved
scene/collection combinations:

| Threads | Samples/s | Useful MiB/s | p95 (s) | EECU-s/success |
|---:|---:|---:|---:|---:|
| 4 | 0.488 | 0.430 | 8.09 | 0.486 |
| 8 | 0.385 | 0.339 | 16.65 | 0.489 |
| 16 | 0.256 | 0.225 | 11.57 | 0.506 |
| 32 | 0.417 | 0.367 | 7.80 | 0.502 |

Sizing the HTTP pool removed the earlier ten-connection-pool warning, but more
threads still did not monotonically improve throughput. Four was fastest in
this one-repetition network state; eight remains a conservative general default
pending repeated regional trials. Post-run diagnosis found that the S2 callback
filtered on the source polygon while the local resolver searched the full patch
footprint; the callback now filters on that same footprint. It also exposed
catalog candidates absent from the preprocessed collection, so those now fall
through to the next ranked candidate. A targeted rerun of the five failures
recovered both affected samples; the remaining three correctly reported no
metadata-qualified scene. The identical original failures do not bias the
relative worker comparison, but its absolute success rate is superseded by this
targeted validation.

Finally, the two-profile scheduler was exercised on eight samples. The second
identity authenticated but lacked `earthengine.computations.create`; its four
leases were retried by the healthy profile and the aggregate run finished 8/8.
The error saved to the summary contains only the profile alias and a redacted
`<project>` placeholder. This validates process isolation, project-level caps,
and failover, but it is not a two-account speed measurement. Grant the service
identity project-level Earth Engine Resource Viewer plus Service Usage Consumer
before repeating the fair 1×8, 1×16, and 2×8 matrix. Total refreshed usage for
the new pixel benchmarks was roughly 0.06 EECU-hours, far below the 100-hour
ceiling.

## v0.3 336-pixel metadata-only rerun

The 336×336 metadata-only case was repeated on 2026-08-21 because its initial
v0.3 throughput was inconsistent with the v0.2 result. The rerun used the first
128 Mining Polygons v2 samples, six S2 bands, the high-volume endpoint, a warm
catalog, spatial/temporal grouping, 16 workers, and three repetitions. Every
timed run had a 100% catalog hit rate and made zero `computeFeatures` calls.

The first rerun reproduced high steady-state throughput but exposed a
deterministic tail. Six samples encountered catalog candidates absent from the
preprocessed collection. Each missing candidate was incorrectly retried five
times with exponential backoff before the engine advanced to the next ranked
candidate. The worst sample accumulated 16 attempts and about 69 seconds even
though its final valid download needed only 2–3 seconds. This reduced the
three-run end-to-end mean to 1.229 samples/s.

The engine now treats that specific missing-image response as non-transient and
advances immediately, while retaining retries for transient network and server
errors. A focused three-repetition validation of all six stragglers completed
18/18 downloads. The former worst case required four candidate attempts and
4.8–7.7 seconds instead of 16 attempts and about 69 seconds.

The corrected full benchmark produced:

| Repetition | Accepted | Elapsed (s) | Samples/s | Useful MiB/s | p95 pixel time (s) | EECU-s/success |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 115/128 | 24.33 | 4.726 | 4.113 | 3.124 | 0.478 |
| 2 | 115/128 | 33.32 | 3.451 | 3.003 | 3.035 | 0.461 |
| 3 | 115/128 | 39.32 | 2.925 | 2.545 | 3.014 | 0.491 |
| **Mean** | **345/384** | — | **3.701** | **3.221** | **3.058** | **0.477** |

The 13 failures per repetition were immediate, valid outcomes with no
metadata-qualified scene. Compared with the v0.2 result of 3.040 samples/s,
the corrected v0.3 mean is 21.7% higher. The earlier v0.3 worker and patch rows
remain useful as records of that live run, but they no longer represent current
336-pixel metadata-only performance. Metadata-only still provides no Cloud
Score validation and remains a speed baseline rather than a production-quality
equivalent to `hybrid_probe`.
