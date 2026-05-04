package aggregation

import (
	"context"
	"math"
	"testing"
	"time"

	pb "github.com/accretional/grpc-server-config/pb/metrics"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// tv is a (unix_seconds, float_value) pair for gauge point expectations.
type tv struct {
	secs int64
	val  float64
}

// dp is a (duration_seconds, numeric_value) pair for delta/cumulative points.
type dp struct {
	durSecs int64
	val     float64
}

// ---------------------------------------------------------------------------
// Series builders
// ---------------------------------------------------------------------------

func makeGauge(pts ...tv) *pb.AnyTimeSeries {
	s := &pb.GaugeTimeSeries{}
	for _, p := range pts {
		s.Points = append(s.Points, &pb.GaugePoint{
			At:    timestamppb.New(time.Unix(p.secs, 0)),
			Value: &pb.TypedValue{Value: &pb.TypedValue_DoubleValue{DoubleValue: p.val}},
		})
	}
	return &pb.AnyTimeSeries{Series: &pb.AnyTimeSeries_Gauge{Gauge: s}}
}

func makeGaugeLabeled(labels map[string]string, pts ...tv) *pb.AnyTimeSeries {
	s := &pb.GaugeTimeSeries{
		Metric: &pb.Metric{Labels: labels},
	}
	for _, p := range pts {
		s.Points = append(s.Points, &pb.GaugePoint{
			At:    timestamppb.New(time.Unix(p.secs, 0)),
			Value: &pb.TypedValue{Value: &pb.TypedValue_DoubleValue{DoubleValue: p.val}},
		})
	}
	return &pb.AnyTimeSeries{Series: &pb.AnyTimeSeries_Gauge{Gauge: s}}
}

func makeDelta(startSecs int64, pts ...dp) *pb.AnyTimeSeries {
	s := &pb.DeltaTimeSeries{
		Start: timestamppb.New(time.Unix(startSecs, 0)),
	}
	for _, p := range pts {
		s.Points = append(s.Points, &pb.NumericPoint{
			Duration: durationpb.New(time.Duration(p.durSecs) * time.Second),
			Value:    &pb.NumericValue{Value: &pb.NumericValue_DoubleValue{DoubleValue: p.val}},
		})
	}
	return &pb.AnyTimeSeries{Series: &pb.AnyTimeSeries_Delta{Delta: s}}
}

func makeDeltaLabeled(labels map[string]string, startSecs int64, pts ...dp) *pb.AnyTimeSeries {
	s := &pb.DeltaTimeSeries{
		Start:  timestamppb.New(time.Unix(startSecs, 0)),
		Metric: &pb.Metric{Labels: labels},
	}
	for _, p := range pts {
		s.Points = append(s.Points, &pb.NumericPoint{
			Duration: durationpb.New(time.Duration(p.durSecs) * time.Second),
			Value:    &pb.NumericValue{Value: &pb.NumericValue_DoubleValue{DoubleValue: p.val}},
		})
	}
	return &pb.AnyTimeSeries{Series: &pb.AnyTimeSeries_Delta{Delta: s}}
}

func makeCumulative(epochSecs int64, pts ...dp) *pb.AnyTimeSeries {
	s := &pb.CumulativeTimeSeries{
		EpochStart: timestamppb.New(time.Unix(epochSecs, 0)),
	}
	for _, p := range pts {
		s.Points = append(s.Points, &pb.NumericPoint{
			Duration: durationpb.New(time.Duration(p.durSecs) * time.Second),
			Value:    &pb.NumericValue{Value: &pb.NumericValue_DoubleValue{DoubleValue: p.val}},
		})
	}
	return &pb.AnyTimeSeries{Series: &pb.AnyTimeSeries_Cumulative{Cumulative: s}}
}

// ---------------------------------------------------------------------------
// Service wrappers
// ---------------------------------------------------------------------------

func mustAlign(t *testing.T, svc *Service, input *pb.AnyTimeSeries, aligner pb.Aligner, periodSecs int64) *pb.AnyTimeSeries {
	t.Helper()
	resp, err := svc.Align(context.Background(), &pb.AlignRequest{
		Input:           input,
		Aligner:         aligner,
		AlignmentPeriod: durationpb.New(time.Duration(periodSecs) * time.Second),
	})
	if err != nil {
		t.Fatalf("Align failed: %v", err)
	}
	return resp.Output
}

// mustAlignWithSmoothingWindow is like mustAlign but also sets
// PercentChangeSmoothingWindow, for testing ALIGN_PERCENT_CHANGE with a
// controlled window size.
func mustAlignWithSmoothingWindow(t *testing.T, svc *Service, input *pb.AnyTimeSeries, aligner pb.Aligner, periodSecs, smoothingWindowSecs int64) *pb.AnyTimeSeries {
	t.Helper()
	resp, err := svc.Align(context.Background(), &pb.AlignRequest{
		Input:                        input,
		Aligner:                      aligner,
		AlignmentPeriod:              durationpb.New(time.Duration(periodSecs) * time.Second),
		PercentChangeSmoothingWindow: durationpb.New(time.Duration(smoothingWindowSecs) * time.Second),
	})
	if err != nil {
		t.Fatalf("Align failed: %v", err)
	}
	return resp.Output
}

func mustReduce(t *testing.T, svc *Service, series []*pb.AnyTimeSeries, reducer pb.Reducer, groupBy []string) []*pb.AnyTimeSeries {
	t.Helper()
	resp, err := svc.Reduce(context.Background(), &pb.ReduceRequest{
		Series:        series,
		Reducer:       reducer,
		GroupByFields: groupBy,
	})
	if err != nil {
		t.Fatalf("Reduce failed: %v", err)
	}
	return resp.Series
}

// ---------------------------------------------------------------------------
// Extractors
// ---------------------------------------------------------------------------

func gaugeOut(t *testing.T, out *pb.AnyTimeSeries) []tv {
	t.Helper()
	g, ok := out.Series.(*pb.AnyTimeSeries_Gauge)
	if !ok {
		t.Fatalf("expected GaugeTimeSeries, got %T", out.Series)
	}
	result := make([]tv, 0, len(g.Gauge.Points))
	for _, p := range g.Gauge.Points {
		f, err := typedToFloat(p.Value)
		if err != nil {
			t.Fatalf("gaugeOut: typedToFloat: %v", err)
		}
		result = append(result, tv{secs: p.At.GetSeconds(), val: f})
	}
	return result
}

func deltaOut(t *testing.T, out *pb.AnyTimeSeries) (startSecs int64, vals []float64, dursSecs []int64) {
	t.Helper()
	d, ok := out.Series.(*pb.AnyTimeSeries_Delta)
	if !ok {
		t.Fatalf("expected DeltaTimeSeries, got %T", out.Series)
	}
	startSecs = d.Delta.Start.GetSeconds()
	for _, p := range d.Delta.Points {
		f, err := numericToFloat(p.Value)
		if err != nil {
			t.Fatalf("deltaOut: numericToFloat: %v", err)
		}
		vals = append(vals, f)
		dursSecs = append(dursSecs, int64(p.Duration.AsDuration().Seconds()))
	}
	return
}

func gaugeLabel(t *testing.T, s *pb.AnyTimeSeries, key string) string {
	t.Helper()
	g, ok := s.Series.(*pb.AnyTimeSeries_Gauge)
	if !ok {
		t.Fatalf("gaugeLabel: expected GaugeTimeSeries, got %T", s.Series)
	}
	if g.Gauge.Metric == nil {
		return ""
	}
	return g.Gauge.Metric.Labels[key]
}

// ---------------------------------------------------------------------------
// Comparison
// ---------------------------------------------------------------------------

// approxEq returns true if a and b are equal within 1e-9 relative tolerance
// or 1e-12 absolute tolerance (for near-zero values).
func approxEq(a, b float64) bool {
	if a == b {
		return true
	}
	diff := math.Abs(a - b)
	if diff < 1e-12 {
		return true
	}
	mag := math.Max(math.Abs(a), math.Abs(b))
	return diff/mag < 1e-9
}

// ---------------------------------------------------------------------------
// Bool gauge builders
// ---------------------------------------------------------------------------

// bv is a (unix_seconds, bool_value) pair for bool gauge point expectations.
type bv struct {
	secs int64
	val  bool
}

func makeGaugeBool(pts ...bv) *pb.AnyTimeSeries {
	s := &pb.GaugeTimeSeries{}
	for _, p := range pts {
		s.Points = append(s.Points, &pb.GaugePoint{
			At:    timestamppb.New(time.Unix(p.secs, 0)),
			Value: &pb.TypedValue{Value: &pb.TypedValue_BoolValue{BoolValue: p.val}},
		})
	}
	return &pb.AnyTimeSeries{Series: &pb.AnyTimeSeries_Gauge{Gauge: s}}
}

// ---------------------------------------------------------------------------
// Distribution gauge builders
// ---------------------------------------------------------------------------

// distPt is a (unix_seconds, *Distribution) pair for distribution gauge points.
type distPt struct {
	secs int64
	dist *pb.Distribution
}

func makeGaugeDist(pts ...distPt) *pb.AnyTimeSeries {
	s := &pb.GaugeTimeSeries{}
	for _, p := range pts {
		s.Points = append(s.Points, &pb.GaugePoint{
			At:    timestamppb.New(time.Unix(p.secs, 0)),
			Value: &pb.TypedValue{Value: &pb.TypedValue_DistributionValue{DistributionValue: p.dist}},
		})
	}
	return &pb.AnyTimeSeries{Series: &pb.AnyTimeSeries_Gauge{Gauge: s}}
}
