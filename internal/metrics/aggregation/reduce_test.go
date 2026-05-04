package aggregation

import (
	"context"
	"testing"

	pb "github.com/accretional/grpc-server-config/pb/metrics"
)

func TestReduce_StatReducers(t *testing.T) {
	svc := New()

	// Two input gauge series (no labels), both with single point at t=60
	s1 := makeGauge(tv{60, 2.0})
	s2 := makeGauge(tv{60, 4.0})

	tests := []struct {
		reducer  pb.Reducer
		expected float64
	}{
		{pb.Reducer_REDUCE_MEAN, 3.0},
		{pb.Reducer_REDUCE_MIN, 2.0},
		{pb.Reducer_REDUCE_MAX, 4.0},
		{pb.Reducer_REDUCE_SUM, 6.0},
		{pb.Reducer_REDUCE_COUNT, 2.0},
		{pb.Reducer_REDUCE_STDDEV, 1.0}, // vals=[2,4], mean=3, ss=(1+1)/2=1
	}

	for _, tt := range tests {
		t.Run(tt.reducer.String(), func(t *testing.T) {
			out := mustReduce(t, svc, []*pb.AnyTimeSeries{s1, s2}, tt.reducer, []string{})
			if len(out) != 1 {
				t.Fatalf("len(out)=%d want 1", len(out))
			}
			pts := gaugeOut(t, out[0])
			if len(pts) != 1 {
				t.Fatalf("len(pts)=%d want 1", len(pts))
			}
			if pts[0].secs != 60 {
				t.Errorf("secs=%d want 60", pts[0].secs)
			}
			if !approxEq(pts[0].val, tt.expected) {
				t.Errorf("val=%v want %v", pts[0].val, tt.expected)
			}
		})
	}
}

func TestReduce_NoOp(t *testing.T) {
	svc := New()

	t.Run("reduce_none_returns_input", func(t *testing.T) {
		out := mustReduce(t, svc, []*pb.AnyTimeSeries{makeGauge(tv{60, 5})}, pb.Reducer_REDUCE_NONE, nil)
		if len(out) != 1 {
			t.Errorf("len(out)=%d want 1", len(out))
		}
	})

	t.Run("empty_series_returns_nil", func(t *testing.T) {
		out := mustReduce(t, svc, []*pb.AnyTimeSeries{}, pb.Reducer_REDUCE_MEAN, nil)
		if len(out) != 0 {
			t.Errorf("len(out)=%d want 0", len(out))
		}
	})
}

func TestReduce_SingleSeriesPassthrough(t *testing.T) {
	svc := New()

	s := makeGauge(tv{60, 7.0})
	out := mustReduce(t, svc, []*pb.AnyTimeSeries{s}, pb.Reducer_REDUCE_MEAN, []string{})
	if len(out) != 1 {
		t.Fatalf("len(out)=%d want 1", len(out))
	}
	pts := gaugeOut(t, out[0])
	if len(pts) != 1 {
		t.Fatalf("len(pts)=%d want 1", len(pts))
	}
	if pts[0].secs != 60 || !approxEq(pts[0].val, 7.0) {
		t.Errorf("pt[0]=%v want {60 7.0}", pts[0])
	}
}

func TestReduce_GroupBy(t *testing.T) {
	svc := New()

	s1 := makeGaugeLabeled(map[string]string{"zone": "east"}, tv{60, 2})
	s2 := makeGaugeLabeled(map[string]string{"zone": "east"}, tv{60, 4})
	s3 := makeGaugeLabeled(map[string]string{"zone": "west"}, tv{60, 8})
	s4 := makeGaugeLabeled(map[string]string{"zone": "west"}, tv{60, 12})

	out := mustReduce(t, svc, []*pb.AnyTimeSeries{s1, s2, s3, s4}, pb.Reducer_REDUCE_MEAN, []string{"zone"})

	if len(out) != 2 {
		t.Fatalf("len(out)=%d want 2", len(out))
	}

	expected := map[string]float64{
		"east": 3.0,
		"west": 10.0,
	}

	for _, series := range out {
		zone := gaugeLabel(t, series, "zone")
		wantVal, ok := expected[zone]
		if !ok {
			t.Errorf("unexpected zone label %q", zone)
			continue
		}
		pts := gaugeOut(t, series)
		if len(pts) != 1 {
			t.Fatalf("zone=%q len(pts)=%d want 1", zone, len(pts))
		}
		if pts[0].secs != 60 {
			t.Errorf("zone=%q secs=%d want 60", zone, pts[0].secs)
		}
		if !approxEq(pts[0].val, wantVal) {
			t.Errorf("zone=%q val=%v want %v", zone, pts[0].val, wantVal)
		}
	}
}

func TestReduce_MissingLabelKey(t *testing.T) {
	svc := New()

	// Policy: a series that does not have the grouped-by label gets "" for that
	// key. All such series — including those with an explicit empty label value —
	// collapse into the same group as each other.
	//
	// s1 has label zone="us-east1" → its own group
	// s2 has no Metric/labels at all → zone="" group
	// s3 has Metric but zone key absent → zone="" group (same as s2)
	s1 := makeGaugeLabeled(map[string]string{"zone": "us-east1"}, tv{60, 10.0})
	s2 := makeGauge(tv{60, 1.0})   // nil Metric
	s3 := makeGaugeLabeled(map[string]string{"region": "us-east1"}, tv{60, 2.0}) // Metric present but no "zone"

	out := mustReduce(t, svc, []*pb.AnyTimeSeries{s1, s2, s3}, pb.Reducer_REDUCE_SUM, []string{"zone"})

	if len(out) != 2 {
		t.Fatalf("output series count = %d, want 2 (one per zone group)", len(out))
	}

	totals := make(map[string]float64)
	for _, series := range out {
		zone := gaugeLabel(t, series, "zone")
		pts := gaugeOut(t, series)
		if len(pts) != 1 {
			t.Fatalf("zone=%q len(pts)=%d want 1", zone, len(pts))
		}
		totals[zone] += pts[0].val
	}

	if !approxEq(totals["us-east1"], 10.0) {
		t.Errorf("zone=us-east1 sum=%v want 10.0", totals["us-east1"])
	}
	// s2 and s3 both map to zone="" and should be summed together
	if !approxEq(totals[""], 3.0) {
		t.Errorf("zone=\"\" sum=%v want 3.0 (missing-label series grouped together)", totals[""])
	}
}

func TestReduce_BoolReducers(t *testing.T) {
	svc := New()

	// Three series at t=60: values 1.0 (true), 0.0 (false), 2.0 (true).
	// applyReducer treats non-zero as true and zero as false.
	s1 := makeGauge(tv{60, 1.0})
	s2 := makeGauge(tv{60, 0.0})
	s3 := makeGauge(tv{60, 2.0})

	cases := []struct {
		reducer  pb.Reducer
		expected float64
	}{
		{pb.Reducer_REDUCE_COUNT_TRUE, 2.0},
		{pb.Reducer_REDUCE_COUNT_FALSE, 1.0},
		{pb.Reducer_REDUCE_FRACTION_TRUE, 2.0 / 3.0},
	}

	for _, tc := range cases {
		t.Run(tc.reducer.String(), func(t *testing.T) {
			out := mustReduce(t, svc, []*pb.AnyTimeSeries{s1, s2, s3}, tc.reducer, nil)
			if len(out) != 1 {
				t.Fatalf("len(out)=%d want 1", len(out))
			}
			pts := gaugeOut(t, out[0])
			if len(pts) != 1 {
				t.Fatalf("len(pts)=%d want 1", len(pts))
			}
			if pts[0].secs != 60 {
				t.Errorf("secs=%d want 60", pts[0].secs)
			}
			if !approxEq(pts[0].val, tc.expected) {
				t.Errorf("reducer %v: val=%v want %v", tc.reducer, pts[0].val, tc.expected)
			}
		})
	}
}

func TestReduce_PercentileReducers(t *testing.T) {
	svc := New()

	// Three series at t=60 with values 10, 20, 30.
	// percentileSlice uses nearest-rank: ceil(p*n)-1
	//   n=3, p=0.99 → ceil(2.97)-1=2 → 30.0
	//   n=3, p=0.95 → ceil(2.85)-1=2 → 30.0
	//   n=3, p=0.50 → ceil(1.5)-1=1  → 20.0
	//   n=3, p=0.05 → ceil(0.15)-1=0 → 10.0
	s1 := makeGauge(tv{60, 10.0})
	s2 := makeGauge(tv{60, 20.0})
	s3 := makeGauge(tv{60, 30.0})

	cases := []struct {
		reducer  pb.Reducer
		expected float64
	}{
		{pb.Reducer_REDUCE_PERCENTILE_99, 30.0},
		{pb.Reducer_REDUCE_PERCENTILE_95, 30.0},
		{pb.Reducer_REDUCE_PERCENTILE_50, 20.0},
		{pb.Reducer_REDUCE_PERCENTILE_05, 10.0},
	}

	for _, tc := range cases {
		t.Run(tc.reducer.String(), func(t *testing.T) {
			out := mustReduce(t, svc, []*pb.AnyTimeSeries{s1, s2, s3}, tc.reducer, nil)
			if len(out) != 1 {
				t.Fatalf("len(out)=%d want 1", len(out))
			}
			pts := gaugeOut(t, out[0])
			if len(pts) != 1 {
				t.Fatalf("len(pts)=%d want 1", len(pts))
			}
			if pts[0].secs != 60 {
				t.Errorf("secs=%d want 60", pts[0].secs)
			}
			if !approxEq(pts[0].val, tc.expected) {
				t.Errorf("reducer %v: val=%v want %v", tc.reducer, pts[0].val, tc.expected)
			}
		})
	}
}

func TestReduce_DeltaGroup(t *testing.T) {
	svc := New()

	t.Run("REDUCE_SUM_two_delta_series", func(t *testing.T) {
		// s1: [10, 20], s2: [30, 40] → SUM: [40, 60]
		s1 := makeDelta(0, dp{60, 10}, dp{60, 20})
		s2 := makeDelta(0, dp{60, 30}, dp{60, 40})
		out := mustReduce(t, svc, []*pb.AnyTimeSeries{s1, s2}, pb.Reducer_REDUCE_SUM, nil)
		if len(out) != 1 {
			t.Fatalf("len(out)=%d want 1", len(out))
		}
		_, vals, durs := deltaOut(t, out[0])
		expected := []float64{40.0, 60.0}
		if len(vals) != len(expected) {
			t.Fatalf("len(vals)=%d want %d", len(vals), len(expected))
		}
		for i, want := range expected {
			if !approxEq(vals[i], want) {
				t.Errorf("vals[%d]=%v want %v", i, vals[i], want)
			}
			if durs[i] != 60 {
				t.Errorf("durs[%d]=%d want 60", i, durs[i])
			}
		}
	})

	t.Run("REDUCE_MEAN_two_delta_series", func(t *testing.T) {
		// s1: [10, 20], s2: [30, 40] → MEAN: [20, 30]
		s1 := makeDelta(0, dp{60, 10}, dp{60, 20})
		s2 := makeDelta(0, dp{60, 30}, dp{60, 40})
		out := mustReduce(t, svc, []*pb.AnyTimeSeries{s1, s2}, pb.Reducer_REDUCE_MEAN, nil)
		if len(out) != 1 {
			t.Fatalf("len(out)=%d want 1", len(out))
		}
		_, vals, _ := deltaOut(t, out[0])
		expected := []float64{20.0, 30.0}
		if len(vals) != len(expected) {
			t.Fatalf("len(vals)=%d want %d", len(vals), len(expected))
		}
		for i, want := range expected {
			if !approxEq(vals[i], want) {
				t.Errorf("vals[%d]=%v want %v", i, vals[i], want)
			}
		}
	})

	t.Run("REDUCE_MIN_two_delta_series", func(t *testing.T) {
		// s1: [10, 20], s2: [30, 40] → MIN: [10, 20]
		s1 := makeDelta(0, dp{60, 10}, dp{60, 20})
		s2 := makeDelta(0, dp{60, 30}, dp{60, 40})
		out := mustReduce(t, svc, []*pb.AnyTimeSeries{s1, s2}, pb.Reducer_REDUCE_MIN, nil)
		if len(out) != 1 {
			t.Fatalf("len(out)=%d want 1", len(out))
		}
		_, vals, _ := deltaOut(t, out[0])
		expected := []float64{10.0, 20.0}
		if len(vals) != len(expected) {
			t.Fatalf("len(vals)=%d want %d", len(vals), len(expected))
		}
		for i, want := range expected {
			if !approxEq(vals[i], want) {
				t.Errorf("vals[%d]=%v want %v", i, vals[i], want)
			}
		}
	})

	t.Run("REDUCE_SUM_delta_with_groupby", func(t *testing.T) {
		// Two east series and one west series → two output groups.
		s1 := makeDeltaLabeled(map[string]string{"zone": "east"}, 0, dp{60, 10}, dp{60, 20})
		s2 := makeDeltaLabeled(map[string]string{"zone": "east"}, 0, dp{60, 30}, dp{60, 40})
		s3 := makeDeltaLabeled(map[string]string{"zone": "west"}, 0, dp{60, 5}, dp{60, 5})
		out := mustReduce(t, svc, []*pb.AnyTimeSeries{s1, s2, s3}, pb.Reducer_REDUCE_SUM, []string{"zone"})
		if len(out) != 2 {
			t.Fatalf("len(out)=%d want 2 (one per zone)", len(out))
		}
	})
}

func TestReduce_Errors(t *testing.T) {
	svc := New()
	ctx := context.Background()

	t.Run("mixed_types_gauge_and_delta", func(t *testing.T) {
		// Both go into the same group (no groupBy).
		// reduceGaugeGroup sees a delta series → error.
		_, err := svc.Reduce(ctx, &pb.ReduceRequest{
			Series:        []*pb.AnyTimeSeries{makeGauge(tv{60, 1.0}), makeDelta(0, dp{60, 1.0})},
			Reducer:       pb.Reducer_REDUCE_SUM,
			GroupByFields: nil,
		})
		if err == nil {
			t.Error("expected error for mixed gauge/delta types in same group, got nil")
		}
	})

	t.Run("delta_mismatched_point_counts", func(t *testing.T) {
		s1 := makeDelta(0, dp{60, 10}, dp{60, 20}) // 2 points
		s2 := makeDelta(0, dp{60, 30})              // 1 point
		_, err := svc.Reduce(ctx, &pb.ReduceRequest{
			Series:        []*pb.AnyTimeSeries{s1, s2},
			Reducer:       pb.Reducer_REDUCE_SUM,
			GroupByFields: nil,
		})
		if err == nil {
			t.Error("expected error for delta series with different point counts, got nil")
		}
	})

	t.Run("cumulative_reduction_not_supported", func(t *testing.T) {
		s1 := makeCumulative(0, dp{60, 100})
		s2 := makeCumulative(0, dp{60, 200})
		_, err := svc.Reduce(ctx, &pb.ReduceRequest{
			Series:        []*pb.AnyTimeSeries{s1, s2},
			Reducer:       pb.Reducer_REDUCE_SUM,
			GroupByFields: nil,
		})
		if err == nil {
			t.Error("expected error for cross-series reduction of CumulativeTimeSeries, got nil")
		}
	})

	t.Run("unknown_series_type", func(t *testing.T) {
		// AnyTimeSeries with nil Series in the first slot triggers the default
		// branch in reduceGroup → "unknown series type".
		_, err := svc.Reduce(ctx, &pb.ReduceRequest{
			Series:        []*pb.AnyTimeSeries{{}, {}},
			Reducer:       pb.Reducer_REDUCE_SUM,
			GroupByFields: nil,
		})
		if err == nil {
			t.Error("expected error for unknown series type, got nil")
		}
	})
}

func TestReduce_MultiFieldGroupBy(t *testing.T) {
	svc := New()

	// Group by ["zone", "env"]. Four series → two distinct groups.
	s1 := makeGaugeLabeled(map[string]string{"zone": "east", "env": "prod"}, tv{60, 10.0})
	s2 := makeGaugeLabeled(map[string]string{"zone": "east", "env": "prod"}, tv{60, 20.0})
	s3 := makeGaugeLabeled(map[string]string{"zone": "west", "env": "staging"}, tv{60, 30.0})
	s4 := makeGaugeLabeled(map[string]string{"zone": "west", "env": "staging"}, tv{60, 40.0})

	out := mustReduce(t, svc, []*pb.AnyTimeSeries{s1, s2, s3, s4}, pb.Reducer_REDUCE_MEAN, []string{"zone", "env"})
	if len(out) != 2 {
		t.Fatalf("len(out)=%d want 2", len(out))
	}

	type key struct{ zone, env string }
	results := make(map[key]float64)
	for _, series := range out {
		g := series.Series.(*pb.AnyTimeSeries_Gauge).Gauge
		zone := g.Metric.Labels["zone"]
		env := g.Metric.Labels["env"]
		pts := gaugeOut(t, series)
		if len(pts) != 1 {
			t.Fatalf("zone=%s env=%s: len(pts)=%d want 1", zone, env, len(pts))
		}
		results[key{zone, env}] = pts[0].val
	}

	if !approxEq(results[key{"east", "prod"}], 15.0) {
		t.Errorf("east/prod mean=%v want 15.0", results[key{"east", "prod"}])
	}
	if !approxEq(results[key{"west", "staging"}], 35.0) {
		t.Errorf("west/staging mean=%v want 35.0", results[key{"west", "staging"}])
	}
}
