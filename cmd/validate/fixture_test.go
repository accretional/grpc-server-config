package main

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/accretional/grpc-server-config/internal/metrics/aggregation"
	pb "github.com/accretional/grpc-server-config/pb/metrics"
	"google.golang.org/protobuf/types/known/durationpb"
)

const fixtureDir = "../../data/validation"

// defaultSmoothing is the GCP default smoothing window for ALIGN_PERCENT_CHANGE.
const defaultSmoothing = 600 * time.Second

// alignFixtureCase describes a single per-series alignment test backed by saved JSON fixtures.
type alignFixtureCase struct {
	name            string
	rawFile         string
	alignedFile     string
	aligner         pb.Aligner
	period          time.Duration
	smoothingWindow time.Duration // only meaningful for ALIGN_PERCENT_CHANGE
}

var alignFixtureCases = []alignFixtureCase{
	// ── GAUGE: cpu utilization ───────────────────────────────────────────────
	{
		name:        "gauge_cpu_ALIGN_MEAN_60s",
		rawFile:     "instance_cpu_utilization_raw.json",
		alignedFile: "instance_cpu_utilization_ALIGN_MEAN_60s.json",
		aligner:     pb.Aligner_ALIGN_MEAN,
		period:      60 * time.Second,
	},
	{
		name:        "gauge_cpu_ALIGN_MEAN_10s",
		rawFile:     "instance_cpu_utilization_raw.json",
		alignedFile: "instance_cpu_utilization_ALIGN_MEAN_10s.json",
		aligner:     pb.Aligner_ALIGN_MEAN,
		period:      10 * time.Second,
	},
	{
		name:        "gauge_cpu_ALIGN_MIN_60s",
		rawFile:     "instance_cpu_utilization_raw.json",
		alignedFile: "instance_cpu_utilization_ALIGN_MIN_60s.json",
		aligner:     pb.Aligner_ALIGN_MIN,
		period:      60 * time.Second,
	},
	{
		name:        "gauge_cpu_ALIGN_MAX_60s",
		rawFile:     "instance_cpu_utilization_raw.json",
		alignedFile: "instance_cpu_utilization_ALIGN_MAX_60s.json",
		aligner:     pb.Aligner_ALIGN_MAX,
		period:      60 * time.Second,
	},
	{
		name:        "gauge_cpu_ALIGN_COUNT_60s",
		rawFile:     "instance_cpu_utilization_raw.json",
		alignedFile: "instance_cpu_utilization_ALIGN_COUNT_60s.json",
		aligner:     pb.Aligner_ALIGN_COUNT,
		period:      60 * time.Second,
	},
	{
		name:        "gauge_cpu_ALIGN_SUM_60s",
		rawFile:     "instance_cpu_utilization_raw.json",
		alignedFile: "instance_cpu_utilization_ALIGN_SUM_60s.json",
		aligner:     pb.Aligner_ALIGN_SUM,
		period:      60 * time.Second,
	},
	{
		name:        "gauge_cpu_ALIGN_STDDEV_60s",
		rawFile:     "instance_cpu_utilization_raw.json",
		alignedFile: "instance_cpu_utilization_ALIGN_STDDEV_60s.json",
		aligner:     pb.Aligner_ALIGN_STDDEV,
		period:      60 * time.Second,
	},
	{
		name:        "gauge_cpu_ALIGN_INTERPOLATE_60s",
		rawFile:     "instance_cpu_utilization_raw.json",
		alignedFile: "instance_cpu_utilization_ALIGN_INTERPOLATE_60s.json",
		aligner:     pb.Aligner_ALIGN_INTERPOLATE,
		period:      60 * time.Second,
	},
	{
		name:        "gauge_cpu_ALIGN_NEXT_OLDER_60s",
		rawFile:     "instance_cpu_utilization_raw.json",
		alignedFile: "instance_cpu_utilization_ALIGN_NEXT_OLDER_60s.json",
		aligner:     pb.Aligner_ALIGN_NEXT_OLDER,
		period:      60 * time.Second,
	},
	{
		name:            "gauge_cpu_ALIGN_PERCENT_CHANGE_60s",
		rawFile:         "instance_cpu_utilization_raw.json",
		alignedFile:     "instance_cpu_utilization_ALIGN_PERCENT_CHANGE_60s.json",
		aligner:         pb.Aligner_ALIGN_PERCENT_CHANGE,
		period:          60 * time.Second,
		smoothingWindow: defaultSmoothing,
	},
	// ── GAUGE: memory ────────────────────────────────────────────────────────
	{
		name:        "gauge_memory_ALIGN_MEAN_60s",
		rawFile:     "instance_memory_balloon_ram_used_raw.json",
		alignedFile: "instance_memory_balloon_ram_used_ALIGN_MEAN_60s.json",
		aligner:     pb.Aligner_ALIGN_MEAN,
		period:      60 * time.Second,
	},
	// ── DELTA: disk write ops ────────────────────────────────────────────────
	{
		name:        "delta_disk_ALIGN_RATE_60s",
		rawFile:     "instance_disk_write_ops_count_raw.json",
		alignedFile: "instance_disk_write_ops_count_ALIGN_RATE_60s.json",
		aligner:     pb.Aligner_ALIGN_RATE,
		period:      60 * time.Second,
	},
	{
		name:        "delta_disk_ALIGN_DELTA_60s",
		rawFile:     "instance_disk_write_ops_count_raw.json",
		alignedFile: "instance_disk_write_ops_count_ALIGN_DELTA_60s.json",
		aligner:     pb.Aligner_ALIGN_DELTA,
		period:      60 * time.Second,
	},
	{
		name:        "delta_disk_ALIGN_MEAN_60s",
		rawFile:     "instance_disk_write_ops_count_raw.json",
		alignedFile: "instance_disk_write_ops_count_ALIGN_MEAN_60s.json",
		aligner:     pb.Aligner_ALIGN_MEAN,
		period:      60 * time.Second,
	},
	{
		name:        "delta_disk_ALIGN_MIN_60s",
		rawFile:     "instance_disk_write_ops_count_raw.json",
		alignedFile: "instance_disk_write_ops_count_ALIGN_MIN_60s.json",
		aligner:     pb.Aligner_ALIGN_MIN,
		period:      60 * time.Second,
	},
	{
		name:        "delta_disk_ALIGN_MAX_60s",
		rawFile:     "instance_disk_write_ops_count_raw.json",
		alignedFile: "instance_disk_write_ops_count_ALIGN_MAX_60s.json",
		aligner:     pb.Aligner_ALIGN_MAX,
		period:      60 * time.Second,
	},
	{
		name:        "delta_disk_ALIGN_COUNT_60s",
		rawFile:     "instance_disk_write_ops_count_raw.json",
		alignedFile: "instance_disk_write_ops_count_ALIGN_COUNT_60s.json",
		aligner:     pb.Aligner_ALIGN_COUNT,
		period:      60 * time.Second,
	},
	{
		name:        "delta_disk_ALIGN_SUM_60s",
		rawFile:     "instance_disk_write_ops_count_raw.json",
		alignedFile: "instance_disk_write_ops_count_ALIGN_SUM_60s.json",
		aligner:     pb.Aligner_ALIGN_SUM,
		period:      60 * time.Second,
	},
	{
		name:        "delta_disk_ALIGN_STDDEV_60s",
		rawFile:     "instance_disk_write_ops_count_raw.json",
		alignedFile: "instance_disk_write_ops_count_ALIGN_STDDEV_60s.json",
		aligner:     pb.Aligner_ALIGN_STDDEV,
		period:      60 * time.Second,
	},
	// ── DELTA: network received bytes ────────────────────────────────────────
	{
		name:        "delta_network_ALIGN_RATE_60s",
		rawFile:     "instance_network_received_bytes_count_raw.json",
		alignedFile: "instance_network_received_bytes_count_ALIGN_RATE_60s.json",
		aligner:     pb.Aligner_ALIGN_RATE,
		period:      60 * time.Second,
	},
	// ── DELTA: uptime ────────────────────────────────────────────────────────
	{
		name:        "delta_uptime_ALIGN_DELTA_60s",
		rawFile:     "instance_uptime_raw.json",
		alignedFile: "instance_uptime_ALIGN_DELTA_60s.json",
		aligner:     pb.Aligner_ALIGN_DELTA,
		period:      60 * time.Second,
	},
	{
		name:        "delta_uptime_ALIGN_RATE_60s",
		rawFile:     "instance_uptime_raw.json",
		alignedFile: "instance_uptime_ALIGN_RATE_60s.json",
		aligner:     pb.Aligner_ALIGN_RATE,
		period:      60 * time.Second,
	},
	// ── CUMULATIVE: cpu usage time ────────────────────────────────────────────
	{
		name:        "cumulative_usage_time_ALIGN_RATE_60s",
		rawFile:     "instance_cpu_usage_time_raw.json",
		alignedFile: "instance_cpu_usage_time_ALIGN_RATE_60s.json",
		aligner:     pb.Aligner_ALIGN_RATE,
		period:      60 * time.Second,
	},
	{
		name:        "cumulative_usage_time_ALIGN_DELTA_60s",
		rawFile:     "instance_cpu_usage_time_raw.json",
		alignedFile: "instance_cpu_usage_time_ALIGN_DELTA_60s.json",
		aligner:     pb.Aligner_ALIGN_DELTA,
		period:      60 * time.Second,
	},
	// ── BOOL: uptime check ────────────────────────────────────────────────────
	{
		name:        "bool_uptime_check_ALIGN_COUNT_TRUE_60s",
		rawFile:     "monitoring_googleapis_com_uptime_check_check_passed_raw.json",
		alignedFile: "monitoring_googleapis_com_uptime_check_check_passed_ALIGN_COUNT_TRUE_60s.json",
		aligner:     pb.Aligner_ALIGN_COUNT_TRUE,
		period:      60 * time.Second,
	},
	{
		name:        "bool_uptime_check_ALIGN_COUNT_FALSE_60s",
		rawFile:     "monitoring_googleapis_com_uptime_check_check_passed_raw.json",
		alignedFile: "monitoring_googleapis_com_uptime_check_check_passed_ALIGN_COUNT_FALSE_60s.json",
		aligner:     pb.Aligner_ALIGN_COUNT_FALSE,
		period:      60 * time.Second,
	},
	{
		name:        "bool_uptime_check_ALIGN_FRACTION_TRUE_60s",
		rawFile:     "monitoring_googleapis_com_uptime_check_check_passed_raw.json",
		alignedFile: "monitoring_googleapis_com_uptime_check_check_passed_ALIGN_FRACTION_TRUE_60s.json",
		aligner:     pb.Aligner_ALIGN_FRACTION_TRUE,
		period:      60 * time.Second,
	},
}

// TestGCP_Fixtures runs every saved fixture pair through our AggregationService
// and asserts point-for-point agreement with GCP's aligned output.
func TestGCP_Fixtures(t *testing.T) {
	svc := aggregation.New()
	ctx := context.Background()

	for _, tc := range alignFixtureCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			raw, err := loadResponse(filepath.Join(fixtureDir, tc.rawFile))
			if err != nil {
				t.Fatalf("load raw fixture: %v", err)
			}
			gcpAligned, err := loadResponse(filepath.Join(fixtureDir, tc.alignedFile))
			if err != nil {
				t.Fatalf("load aligned fixture: %v", err)
			}

			smoothing := tc.smoothingWindow
			if smoothing == 0 {
				smoothing = defaultSmoothing
			}

			gcpIdx := indexByLabels(gcpAligned.TimeSeries)
			totalComparable, totalMatched, totalMismatches := 0, 0, 0
			seriesCompared := 0

			for _, rawTS := range raw.TimeSeries {
				key := fullSeriesKey(&rawTS)
				gcpTS, ok := gcpIdx[key]
				if !ok {
					continue
				}

				ourInput, err := convertTimeSeries(&rawTS)
				if err != nil {
					t.Errorf("convert series %v: %v", rawTS.Metric.Labels, err)
					continue
				}

				resp, err := svc.Align(ctx, &pb.AlignRequest{
					Input:                        ourInput,
					Aligner:                      tc.aligner,
					AlignmentPeriod:              durationpb.New(tc.period),
					PercentChangeSmoothingWindow: durationpb.New(smoothing),
				})
				if err != nil {
					t.Errorf("align series %v: %v", rawTS.Metric.Labels, err)
					continue
				}

				ourPoints := extractPoints(resp.Output)
				rawMin, rawMax := rawTimeRange(&rawTS)
				gcpPoints := trimGCPPoints(gcpAlignedPoints(gcpTS), rawMin, rawMax, tc.aligner, smoothing)

				matched, rep := diffPoints(ourPoints, gcpPoints)
				totalComparable += len(gcpPoints)
				totalMatched += matched
				totalMismatches += rep.countMismatch
				seriesCompared++

				for _, ex := range rep.examples {
					t.Errorf("series %v: at=%s gcp=%v ours=%v absDiff=%v",
						rawTS.Metric.Labels, ex.at.Format(time.RFC3339), ex.gcp, ex.ours, ex.absDiff)
				}
			}

			if totalComparable == 0 {
				t.Skipf("raw and aligned fixtures have non-overlapping time windows; re-fetch with: " +
					"go run ./cmd/validate -refresh")
			}
			if totalMismatches > 0 {
				t.Errorf("%d mismatches across %d series: %d/%d points matched",
					totalMismatches, seriesCompared, totalMatched, totalComparable)
			}
			t.Logf("matched %d/%d comparable points across %d series", totalMatched, totalComparable, seriesCompared)
		})
	}
}

// reduceFixtureCase describes a two-step align+reduce fixture test.
type reduceFixtureCase struct {
	name          string
	rawFile       string
	alignedFile   string
	reducedFile   string
	aligner       pb.Aligner
	period        time.Duration
	reducer       pb.Reducer
	groupByFields []string
}

var reduceFixtureCases = []reduceFixtureCase{
	// ── GAUGE: cpu utilization — statistical reducers grouped by zone ─────────
	{
		name:          "cpu_REDUCE_MEAN",
		rawFile:       "instance_cpu_utilization_raw.json",
		alignedFile:   "instance_cpu_utilization_ALIGN_MEAN_60s.json",
		reducedFile:   "instance_cpu_utilization_ALIGN_MEAN_REDUCE_MEAN_60s.json",
		aligner:       pb.Aligner_ALIGN_MEAN,
		period:        60 * time.Second,
		reducer:       pb.Reducer_REDUCE_MEAN,
		groupByFields: []string{"zone"},
	},
	{
		name:          "cpu_REDUCE_MIN",
		rawFile:       "instance_cpu_utilization_raw.json",
		alignedFile:   "instance_cpu_utilization_ALIGN_MEAN_60s.json",
		reducedFile:   "instance_cpu_utilization_ALIGN_MEAN_REDUCE_MIN_60s.json",
		aligner:       pb.Aligner_ALIGN_MEAN,
		period:        60 * time.Second,
		reducer:       pb.Reducer_REDUCE_MIN,
		groupByFields: []string{"zone"},
	},
	{
		name:          "cpu_REDUCE_MAX",
		rawFile:       "instance_cpu_utilization_raw.json",
		alignedFile:   "instance_cpu_utilization_ALIGN_MEAN_60s.json",
		reducedFile:   "instance_cpu_utilization_ALIGN_MEAN_REDUCE_MAX_60s.json",
		aligner:       pb.Aligner_ALIGN_MEAN,
		period:        60 * time.Second,
		reducer:       pb.Reducer_REDUCE_MAX,
		groupByFields: []string{"zone"},
	},
	{
		name:          "cpu_REDUCE_SUM",
		rawFile:       "instance_cpu_utilization_raw.json",
		alignedFile:   "instance_cpu_utilization_ALIGN_MEAN_60s.json",
		reducedFile:   "instance_cpu_utilization_ALIGN_MEAN_REDUCE_SUM_60s.json",
		aligner:       pb.Aligner_ALIGN_MEAN,
		period:        60 * time.Second,
		reducer:       pb.Reducer_REDUCE_SUM,
		groupByFields: []string{"zone"},
	},
	{
		name:          "cpu_REDUCE_COUNT",
		rawFile:       "instance_cpu_utilization_raw.json",
		alignedFile:   "instance_cpu_utilization_ALIGN_MEAN_60s.json",
		reducedFile:   "instance_cpu_utilization_ALIGN_MEAN_REDUCE_COUNT_60s.json",
		aligner:       pb.Aligner_ALIGN_MEAN,
		period:        60 * time.Second,
		reducer:       pb.Reducer_REDUCE_COUNT,
		groupByFields: []string{"zone"},
	},
	{
		name:          "cpu_REDUCE_STDDEV",
		rawFile:       "instance_cpu_utilization_raw.json",
		alignedFile:   "instance_cpu_utilization_ALIGN_MEAN_60s.json",
		reducedFile:   "instance_cpu_utilization_ALIGN_MEAN_REDUCE_STDDEV_60s.json",
		aligner:       pb.Aligner_ALIGN_MEAN,
		period:        60 * time.Second,
		reducer:       pb.Reducer_REDUCE_STDDEV,
		groupByFields: []string{"zone"},
	},
	// ── BOOL: uptime check — bool reducers ───────────────────────────────────
	{
		name:          "uptime_check_REDUCE_COUNT_TRUE",
		rawFile:       "monitoring_googleapis_com_uptime_check_check_passed_raw.json",
		alignedFile:   "monitoring_googleapis_com_uptime_check_check_passed_ALIGN_NEXT_OLDER_60s.json",
		reducedFile:   "monitoring_googleapis_com_uptime_check_check_passed_ALIGN_NEXT_OLDER_REDUCE_COUNT_TRUE_60s.json",
		aligner:       pb.Aligner_ALIGN_NEXT_OLDER,
		period:        60 * time.Second,
		reducer:       pb.Reducer_REDUCE_COUNT_TRUE,
		groupByFields: []string{},
	},
	{
		name:          "uptime_check_REDUCE_COUNT_FALSE",
		rawFile:       "monitoring_googleapis_com_uptime_check_check_passed_raw.json",
		alignedFile:   "monitoring_googleapis_com_uptime_check_check_passed_ALIGN_NEXT_OLDER_60s.json",
		reducedFile:   "monitoring_googleapis_com_uptime_check_check_passed_ALIGN_NEXT_OLDER_REDUCE_COUNT_FALSE_60s.json",
		aligner:       pb.Aligner_ALIGN_NEXT_OLDER,
		period:        60 * time.Second,
		reducer:       pb.Reducer_REDUCE_COUNT_FALSE,
		groupByFields: []string{},
	},
	{
		name:          "uptime_check_REDUCE_FRACTION_TRUE",
		rawFile:       "monitoring_googleapis_com_uptime_check_check_passed_raw.json",
		alignedFile:   "monitoring_googleapis_com_uptime_check_check_passed_ALIGN_NEXT_OLDER_60s.json",
		reducedFile:   "monitoring_googleapis_com_uptime_check_check_passed_ALIGN_NEXT_OLDER_REDUCE_FRACTION_TRUE_60s.json",
		aligner:       pb.Aligner_ALIGN_NEXT_OLDER,
		period:        60 * time.Second,
		reducer:       pb.Reducer_REDUCE_FRACTION_TRUE,
		groupByFields: []string{},
	},
}

// TestGCP_ReduceFixtures runs every saved reduce fixture through our two-step
// align+reduce pipeline and asserts point-for-point agreement with GCP's output.
func TestGCP_ReduceFixtures(t *testing.T) {
	svc := aggregation.New()
	ctx := context.Background()

	for _, tc := range reduceFixtureCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			raw, err := loadResponse(filepath.Join(fixtureDir, tc.rawFile))
			if err != nil {
				t.Skipf("fixture not found (run: go run ./cmd/validate -refresh): %v", err)
			}
			gcpAligned, err := loadResponse(filepath.Join(fixtureDir, tc.alignedFile))
			if err != nil {
				t.Skipf("fixture not found (run: go run ./cmd/validate -refresh): %v", err)
			}
			gcpReduced, err := loadResponse(filepath.Join(fixtureDir, tc.reducedFile))
			if err != nil {
				t.Skipf("fixture not found (run: go run ./cmd/validate -refresh): %v", err)
			}

			gcpAlignedIdx := indexByLabels(gcpAligned.TimeSeries)
			var aligned []*pb.AnyTimeSeries

			for _, rawTS := range raw.TimeSeries {
				if _, ok := gcpAlignedIdx[fullSeriesKey(&rawTS)]; !ok {
					continue
				}
				ourInput, err := convertTimeSeries(&rawTS)
				if err != nil {
					t.Errorf("convert series %v: %v", rawTS.Metric.Labels, err)
					continue
				}
				resp, err := svc.Align(ctx, &pb.AlignRequest{
					Input:           ourInput,
					Aligner:         tc.aligner,
					AlignmentPeriod: durationpb.New(tc.period),
				})
				if err != nil {
					t.Errorf("align series %v: %v", rawTS.Metric.Labels, err)
					continue
				}
				aligned = append(aligned, resp.Output)
			}
			if len(aligned) == 0 {
				t.Skipf("no series aligned — raw and aligned fixtures may have non-overlapping windows; re-fetch with: go run ./cmd/validate -refresh")
			}

			reduceResp, err := svc.Reduce(ctx, &pb.ReduceRequest{
				Series:        aligned,
				Reducer:       tc.reducer,
				GroupByFields: tc.groupByFields,
			})
			if err != nil {
				t.Fatalf("reduce: %v", err)
			}
			if len(reduceResp.Series) == 0 {
				t.Fatal("reduce produced no output series")
			}

			gcpReducedIdx := make(map[string]gcpTimeSeries, len(gcpReduced.TimeSeries))
			for _, ts := range gcpReduced.TimeSeries {
				gcpReducedIdx[gcpReducedSeriesKey(&ts, tc.groupByFields)] = ts
			}

			totalMatched, totalComparable, totalMismatches := 0, 0, 0
			for _, ourSeries := range reduceResp.Series {
				labels := seriesOutputLabels(ourSeries, tc.groupByFields)
				gcpTS, ok := gcpReducedIdx[labelsKey(labels)]
				if !ok {
					t.Errorf("our reduced series (labels=%v) has no matching GCP counterpart", labels)
					continue
				}
				ourPoints := extractPoints(ourSeries)
				gcpPoints := gcpAlignedPoints(gcpTS)
				matched, rep := diffPoints(ourPoints, gcpPoints)
				totalMatched += matched
				totalComparable += len(gcpPoints)
				totalMismatches += rep.countMismatch
				for _, ex := range rep.examples {
					t.Errorf("reduced series %v: at=%s gcp=%v ours=%v absDiff=%v",
						labels, ex.at.Format(time.RFC3339), ex.gcp, ex.ours, ex.absDiff)
				}
			}

			if totalComparable == 0 {
				t.Skipf("no comparable points found; re-fetch with: go run ./cmd/validate -refresh")
			}
			if totalMismatches > 0 {
				t.Errorf("%d mismatches: %d/%d points matched", totalMismatches, totalMatched, totalComparable)
			}
			t.Logf("reduce: matched %d/%d points across %d output series",
				totalMatched, totalComparable, len(reduceResp.Series))
		})
	}
}
