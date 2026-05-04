# Testing

## Unit tests

```
go test ./...
```


---

## Integration: GCP preset validation

`scripts/validate_presets.sh` fetches live data from GCP Cloud Monitoring, runs every aligner/reducer preset through our `AggregationService`, and diffs the output point-by-point against what GCP returned.

### Requirements

- `gcloud` CLI authenticated (`gcloud auth login`)
- `jq` installed (`brew install jq`)
- Access to the target GCP project

### Running

```bash
# Full run (uses the active gcloud project)
./scripts/validate_presets.sh

# Override project or look-back window
./scripts/validate_presets.sh --project=my-project --hours=2

# Reuse previously fetched data (skip the GCP fetch step)
./scripts/validate_presets.sh --skip-fetch
```

Results are written to `scripts/results/<timestamp>/` as JSON and a `summary.json`.

### Expected output

```
PRESET           SERIES  MATCHED_PTS          MISMATCHES  MAX_ABS_DIFF  AVG_ALN_µs  STATUS
cpu                  13  741/741                       0  0                     10    PASS
cpu-pct-change       13  611/611                       0  0                      7    PASS
network              13  754/754                       0  0                     11    PASS
memory                6  342/342                       0  0                      9    PASS
uptime               13  754/754                       0  0                     10    PASS
cpu-by-zone          13  741/741 +57/57                0  0                     10    PASS

  6 presets   6 passed   0 failed
```

`MATCHED_PTS` shows `matched/comparable`. For `cpu-by-zone` the `+57/57` is the cross-series reduce result appended after the per-series count.

### Boundary points excluded from comparison

The `comparable` count is lower than the total GCP output points for some presets. These boundary points are intentionally excluded and do not count as mismatches.

**Start boundary in `ALIGN_PERCENT_CHANGE` only.**
The algorithm computes `((mean(T−W, T] − mean(T−W−P, T−P]) / |prev|) × 100`, where `W` is the smoothing window (default 10 min) and `P` is the alignment period (60 s). At time `T`, the previous window needs raw data going back to `T−W`. The raw API only returns data from the first available point (`rawMin`), so points where `T < rawMin + W` cannot be fairly compared (GCP's aligned endpoint uses internal pre-history that the raw API does not expose). The comparison tool skips these (typically the first 10 points per series per fetch).

**End boundary for all presets.**
The GCP aligned API runs a few minutes closer to real-time than our raw fetch window ends, producing 1–3 extra aligned points per series beyond `rawMax`. These are also excluded.

Our algorithm matches GCP exactly (floating-point diff ≤ 3e-14) for every point within the comparable window.
