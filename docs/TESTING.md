# AggregationService — Comparison Testing Against GCP

We validate our `AggregationService` implementation by fetching real metrics from GCP Cloud Monitoring and comparing our aligned output point-by-point against GCP's.

## How it works

```
cmd/compare --fetch    # pulls raw + GCP-aligned data from Cloud Monitoring REST API → testdata/
cmd/compare --compare  # runs our aligner on the raw data, diffs against GCP's output
```

Raw and aligned data are saved as JSON in `cmd/compare/testdata/`. Both are fetched in the same run to ensure identical time windows. Tolerance: abs < 1e-9 AND rel < 1e-6 (floating-point noise only).

---

## Results

All tests run against a real GCP project (`scratchpad-427517`), 1-hour window, 60s alignment period, 13 VM series per metric.

### GAUGE — `compute.googleapis.com/instance/cpu/utilization`

| Aligner | Points matched | Our latency (avg/series) |
|---|---|---|
| ALIGN_MEAN | 741/741 ✓ | 9µs |
| ALIGN_MIN | 741/741 ✓ | 8µs |
| ALIGN_MAX | 741/741 ✓ | 8µs |
| ALIGN_COUNT | 741/741 ✓ | 9µs |
| ALIGN_STDDEV | 741/741 ✓ | 10µs |
| ALIGN_INTERPOLATE | 754/754 ✓ | 6µs |
| ALIGN_NEXT_OLDER | 754/754 ✓ | 1µs |

### DELTA — `compute.googleapis.com/instance/disk/write_ops_count`

| Aligner | Points matched | Our latency (avg/series) |
|---|---|---|
| ALIGN_RATE | 754/754 ✓ | 9µs |
| ALIGN_SUM | 754/754 ✓ | 9µs |
| ALIGN_MEAN | 754/754 ✓ | 17µs |
| ALIGN_MIN | 754/754 ✓ | 9µs |
| ALIGN_MAX | 754/754 ✓ | 12µs |
| ALIGN_COUNT | 754/754 ✓ | 9µs |
| ALIGN_STDDEV | 754/754 ✓ | 9µs |
| ALIGN_DELTA | 754/754 ✓ | 8µs |

### DELTA — `compute.googleapis.com/instance/uptime`

| Aligner | Points matched | Our latency (avg/series) |
|---|---|---|
| ALIGN_RATE | 741/741 ✓ | 8µs |
| ALIGN_DELTA | 741/741 ✓ | 11µs |

**17 out of 17 aligner/metric combinations: exact match.**

---

## Latency

| | Latency |
|---|---|
| GCP Cloud Monitoring API (raw fetch) | ~900ms |
| GCP Cloud Monitoring API (aligned fetch) | ~650ms |
| Our `Align` call (per series, 57 raw points) | ~1–17µs |

Our per-series compute is ~100,000× faster than GCP's API round trip. This comparison is not apples-to-apples: GCP's number includes network, authentication, and serving overhead. GCP's [Monarch](https://research.google/pubs/monarch-googles-planet-scale-in-memory-time-series-database/) architecture maintains aligned results incrementally at write time (standing queries), so the actual server-side computation is near-zero. Our implementation computes on demand at read time, which works well at small scale.

---

## Bugs found and fixed

**Bucket boundary convention (GAUGE).** Our initial implementation used left-closed buckets `[T, T+period)`. GCP uses right-closed `(T-period, T]`. Every value was shifted one bucket late. Fixed `bucketIdx` from `floor(T/ps)` to `ceil(T/ps) - 1`.

**GCP raw DELTA data has a 1ms offset.** Raw DELTA windows arrive as `[T+1ms, T+period]` rather than `[T, T+period]`. This is GCP's internal convention to avoid endpoint ambiguity. We snap to whole seconds (`Truncate`/`Round`) on ingest so durations are exactly 60s and our rate arithmetic matches GCP's.

**NEXT_OLDER boundary.** The original loop used `< bucketEnd`, which excluded raw points exactly on a boundary. Changed to `<=`. Also extended the output range by one carry-forward bucket past the last raw point, matching GCP's behaviour.

**INTERPOLATE range.** GCP extrapolates one bucket beyond the last raw point using linear extrapolation from the last two points. Extended our range by `+1` and added extrapolation support to `interpolateGaugeAt`.

**ALIGN_DELTA on DELTA data.** Our `alignDelta` dispatch didn't handle `ALIGN_DELTA` — it fell through to `applyStat` which errored. Added `alignDeltaRebucket`: sums raw windows proportionally into alignment buckets and returns a `DeltaTimeSeries` (matching GCP's output kind).

---

## Not yet verified

| Aligner | Reason |
|---|---|
| ALIGN_PERCENT_CHANGE | GCP's formula doesn't match `(v_T - v_(T-period)) / v_(T-period) * 100`. Suspected to use half-window means or data outside the query window. Needs investigation. |
| ALIGN_COUNT_TRUE / COUNT_FALSE / FRACTION_TRUE | Requires a boolean-valued metric. No suitable metric tested yet. |
| ALIGN_PERCENTILE_99/95/50/05 | Requires a distribution-valued metric. Not tested yet. |
| Cross-series Reduce | `ReduceRequest` has no comparison tool yet. |

---

## Running it yourself

```bash
# Fetch data (needs gcloud auth)
go run ./cmd/compare --fetch --compare \
  --project=<your-project> \
  --metric=compute.googleapis.com/instance/cpu/utilization \
  --aligner=ALIGN_MEAN

# Compare from saved data (no GCP access needed)
go run ./cmd/compare --compare \
  --metric=compute.googleapis.com/instance/cpu/utilization \
  --aligner=ALIGN_MEAN
```

Always run `--fetch` and `--compare` together (or in the same minute) to keep raw and aligned files time-synchronized. The raw file is shared across all aligners for a given metric — fetching one aligner overwrites the raw file.
