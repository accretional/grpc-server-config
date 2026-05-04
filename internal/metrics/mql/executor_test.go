package mql

import (
	"context"
	"errors"
	"testing"
	"time"

	pb "github.com/accretional/grpc-server-config/pb/metrics"
	mqlpb "github.com/accretional/grpc-server-config/pb/metrics"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ---------------------------------------------------------------------------
// mockReader
// ---------------------------------------------------------------------------

type mockReader struct {
	series []*pb.AnyTimeSeries
	err    error

	// captured call args
	gotResourceType string
	gotMetricType   string
	gotStart        time.Time
	gotEnd          time.Time
	gotFilter       *mqlpb.Predicate
}

func (m *mockReader) ReadTimeSeries(
	_ context.Context,
	resourceType, metricType string,
	start, end time.Time,
	filter *mqlpb.Predicate,
) ([]*pb.AnyTimeSeries, error) {
	m.gotResourceType = resourceType
	m.gotMetricType = metricType
	m.gotStart = start
	m.gotEnd = end
	m.gotFilter = filter
	return m.series, m.err
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func singleGauge(secs int64, val float64) *pb.AnyTimeSeries {
	return &pb.AnyTimeSeries{
		Series: &pb.AnyTimeSeries_Gauge{
			Gauge: &pb.GaugeTimeSeries{
				Points: []*pb.GaugePoint{
					{
						At:    timestamppb.New(time.Unix(secs, 0).UTC()),
						Value: &pb.TypedValue{Value: &pb.TypedValue_DoubleValue{DoubleValue: val}},
					},
				},
			},
		},
	}
}

func labeledGauge(labels map[string]string, secs int64, val float64) *pb.AnyTimeSeries {
	return &pb.AnyTimeSeries{
		Series: &pb.AnyTimeSeries_Gauge{
			Gauge: &pb.GaugeTimeSeries{
				Metric: &pb.Metric{Labels: labels},
				Points: []*pb.GaugePoint{
					{
						At:    timestamppb.New(time.Unix(secs, 0).UTC()),
						Value: &pb.TypedValue{Value: &pb.TypedValue_DoubleValue{DoubleValue: val}},
					},
				},
			},
		},
	}
}

func fetchPlan(resourceType, metricType string) *mqlpb.QueryPlan {
	return &mqlpb.QueryPlan{
		Fetch: &mqlpb.FetchOp{
			ResourceType: resourceType,
			MetricType:   metricType,
		},
	}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestExecute_NoFetch(t *testing.T) {
	r := &mockReader{}
	ex := NewExecutor(r)
	_, err := ex.Execute(context.Background(), &mqlpb.QueryPlan{})
	if err == nil {
		t.Fatal("expected error for plan with no fetch, got nil")
	}
}

func TestExecute_EmptyPipeline(t *testing.T) {
	series := []*pb.AnyTimeSeries{singleGauge(60, 1.0), singleGauge(120, 2.0)}
	r := &mockReader{series: series}
	ex := NewExecutor(r)

	plan := fetchPlan("gce_instance", "compute.googleapis.com/instance/cpu/utilization")
	res, err := ex.Execute(context.Background(), plan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(res.Series) != 2 {
		t.Errorf("series count = %d, want 2", len(res.Series))
	}
	if r.gotResourceType != "gce_instance" {
		t.Errorf("resourceType = %q, want gce_instance", r.gotResourceType)
	}
	if r.gotMetricType != "compute.googleapis.com/instance/cpu/utilization" {
		t.Errorf("metricType = %q", r.gotMetricType)
	}
}

func TestExecute_FilterPushdown(t *testing.T) {
	pred := &mqlpb.Predicate{
		Expr: &mqlpb.Predicate_Comparison{
			Comparison: &mqlpb.ComparisonExpr{
				LabelPath: "metric.labels.zone",
				Op:        mqlpb.ComparisonOp_EQ,
				Rhs:       &mqlpb.Value{V: &mqlpb.Value_StringValue{StringValue: "us-central1-a"}},
			},
		},
	}
	r := &mockReader{series: []*pb.AnyTimeSeries{singleGauge(60, 1.0)}}
	ex := NewExecutor(r)

	plan := fetchPlan("gce_instance", "compute.googleapis.com/instance/cpu/utilization")
	plan.Pipeline = []*mqlpb.PipeOp{
		{Op: &mqlpb.PipeOp_Filter{Filter: &mqlpb.FilterOp{Predicate: pred}}},
	}

	if _, err := ex.Execute(context.Background(), plan); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.gotFilter == nil {
		t.Fatal("expected filter predicate to be passed to reader, got nil")
	}
	cmp := r.gotFilter.GetComparison()
	if cmp == nil || cmp.LabelPath != "metric.labels.zone" {
		t.Errorf("filter predicate = %v, want comparison on metric.labels.zone", r.gotFilter)
	}
}

func TestExecute_AlignStage(t *testing.T) {
	// Two points 60s apart — ALIGN_MEAN with 60s period should produce aligned output.
	raw := &pb.AnyTimeSeries{
		Series: &pb.AnyTimeSeries_Gauge{
			Gauge: &pb.GaugeTimeSeries{
				Points: []*pb.GaugePoint{
					{At: timestamppb.New(time.Unix(60, 0).UTC()), Value: &pb.TypedValue{Value: &pb.TypedValue_DoubleValue{DoubleValue: 2.0}}},
					{At: timestamppb.New(time.Unix(90, 0).UTC()), Value: &pb.TypedValue{Value: &pb.TypedValue_DoubleValue{DoubleValue: 4.0}}},
				},
			},
		},
	}
	r := &mockReader{series: []*pb.AnyTimeSeries{raw}}
	ex := NewExecutor(r)

	plan := fetchPlan("gce_instance", "compute.googleapis.com/instance/cpu/utilization")
	plan.Pipeline = []*mqlpb.PipeOp{
		{Op: &mqlpb.PipeOp_Align{Align: &mqlpb.AlignOp{
			Aligner: pb.Aligner_ALIGN_MEAN,
			Period:  durationpb.New(60 * time.Second),
		}}},
	}

	res, err := ex.Execute(context.Background(), plan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(res.Series) != 1 {
		t.Fatalf("series count = %d, want 1", len(res.Series))
	}
	g := res.Series[0].GetGauge()
	if g == nil {
		t.Fatal("expected GaugeTimeSeries after align")
	}
	if len(g.Points) == 0 {
		t.Error("expected at least one aligned point")
	}
}

func TestExecute_GroupByStage(t *testing.T) {
	// Two series with the same label value collapse to one under REDUCE_SUM.
	ts := int64(60)
	s1 := labeledGauge(map[string]string{"zone": "us-east1"}, ts, 1.0)
	s2 := labeledGauge(map[string]string{"zone": "us-east1"}, ts, 3.0)

	r := &mockReader{series: []*pb.AnyTimeSeries{s1, s2}}
	ex := NewExecutor(r)

	plan := fetchPlan("gce_instance", "compute.googleapis.com/instance/cpu/utilization")
	plan.Pipeline = []*mqlpb.PipeOp{
		{Op: &mqlpb.PipeOp_GroupBy{GroupBy: &mqlpb.GroupByOp{
			LabelKeys: []string{"metric.labels.zone"},
			Reducer:   pb.Reducer_REDUCE_SUM,
		}}},
	}

	res, err := ex.Execute(context.Background(), plan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(res.Series) != 1 {
		t.Errorf("series count after group_by = %d, want 1", len(res.Series))
	}
}

func TestExecute_WithinSetsTimeRange(t *testing.T) {
	r := &mockReader{series: nil}
	ex := NewExecutor(r)

	plan := fetchPlan("gce_instance", "compute.googleapis.com/instance/cpu/utilization")
	plan.TimeRange = &mqlpb.TimeRange{
		Range: &mqlpb.TimeRange_Relative{Relative: durationpb.New(30 * time.Minute)},
	}

	before := time.Now().UTC()
	if _, err := ex.Execute(context.Background(), plan); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	after := time.Now().UTC()

	window := r.gotEnd.Sub(r.gotStart)
	if window != 30*time.Minute {
		t.Errorf("time window = %v, want 30m", window)
	}
	// end should be truncated to the minute, within the call bounds
	if r.gotEnd.Before(before.Truncate(time.Minute)) || r.gotEnd.After(after) {
		t.Errorf("end time %v is outside expected range [%v, %v]", r.gotEnd, before.Truncate(time.Minute), after)
	}
}

func TestExecute_DefaultTimeWindow(t *testing.T) {
	r := &mockReader{series: nil}
	ex := NewExecutor(r)

	if _, err := ex.Execute(context.Background(), fetchPlan("r", "m")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	window := r.gotEnd.Sub(r.gotStart)
	if window != time.Hour {
		t.Errorf("default window = %v, want 1h", window)
	}
}

func TestExecute_ReaderError(t *testing.T) {
	sentinel := errors.New("storage unavailable")
	r := &mockReader{err: sentinel}
	ex := NewExecutor(r)

	_, err := ex.Execute(context.Background(), fetchPlan("r", "m"))
	if err == nil {
		t.Fatal("expected error from reader, got nil")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("error = %v, want to wrap %v", err, sentinel)
	}
}

func TestExecute_AlignErrorPropagates(t *testing.T) {
	r := &mockReader{series: []*pb.AnyTimeSeries{singleGauge(60, 1.0)}}
	ex := NewExecutor(r)

	plan := fetchPlan("r", "m")
	plan.Pipeline = []*mqlpb.PipeOp{
		{Op: &mqlpb.PipeOp_Align{Align: &mqlpb.AlignOp{
			Aligner: pb.Aligner_ALIGN_MEAN,
			Period:  durationpb.New(0), // zero period is invalid
		}}},
	}

	_, err := ex.Execute(context.Background(), plan)
	if err == nil {
		t.Fatal("expected error for zero alignment period, got nil")
	}
}
