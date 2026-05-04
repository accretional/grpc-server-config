package mql

import (
	"testing"
	"time"

	pb "github.com/accretional/grpc-server-config/pb/metrics"
	mqlpb "github.com/accretional/grpc-server-config/pb/metrics"
)

func TestParse_BasicFetch(t *testing.T) {
	plan, err := Parse("fetch gce_instance::compute.googleapis.com/instance/cpu/utilization")
	if err != nil {
		t.Fatal(err)
	}
	if plan.Fetch.ResourceType != "gce_instance" {
		t.Errorf("resource_type = %q, want gce_instance", plan.Fetch.ResourceType)
	}
	if plan.Fetch.MetricType != "compute.googleapis.com/instance/cpu/utilization" {
		t.Errorf("metric_type = %q", plan.Fetch.MetricType)
	}
	if len(plan.Pipeline) != 0 {
		t.Errorf("expected empty pipeline, got %d stages", len(plan.Pipeline))
	}
}

func TestParse_FullPipeline(t *testing.T) {
	q := `
		fetch gce_instance::compute.googleapis.com/instance/cpu/utilization
		| filter metric.labels.instance_name =~ "prod-.*"
		| align mean(1m)
		| group_by [metric.labels.zone], mean()
		| within 1h
	`
	plan, err := Parse(q)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	if plan.Fetch.ResourceType != "gce_instance" {
		t.Errorf("resource_type = %q", plan.Fetch.ResourceType)
	}

	// Expect 3 pipeline stages: filter, align, group_by
	if len(plan.Pipeline) != 3 {
		t.Fatalf("pipeline len = %d, want 3", len(plan.Pipeline))
	}

	// Stage 0: filter
	filterOp, ok := plan.Pipeline[0].Op.(*mqlpb.PipeOp_Filter)
	if !ok {
		t.Fatalf("stage 0: expected FilterOp, got %T", plan.Pipeline[0].Op)
	}
	cmp := filterOp.Filter.Predicate.GetComparison()
	if cmp == nil {
		t.Fatal("expected comparison predicate")
	}
	if cmp.LabelPath != "metric.labels.instance_name" {
		t.Errorf("label_path = %q", cmp.LabelPath)
	}
	if cmp.Op != mqlpb.ComparisonOp_RE {
		t.Errorf("op = %v, want RE", cmp.Op)
	}
	if cmp.Rhs.GetStringValue() != "prod-.*" {
		t.Errorf("rhs = %q", cmp.Rhs.GetStringValue())
	}

	// Stage 1: align
	alignOp, ok := plan.Pipeline[1].Op.(*mqlpb.PipeOp_Align)
	if !ok {
		t.Fatalf("stage 1: expected AlignOp, got %T", plan.Pipeline[1].Op)
	}
	if alignOp.Align.Aligner != pb.Aligner_ALIGN_MEAN {
		t.Errorf("aligner = %v", alignOp.Align.Aligner)
	}
	if alignOp.Align.Period.AsDuration() != time.Minute {
		t.Errorf("period = %v, want 1m", alignOp.Align.Period.AsDuration())
	}

	// Stage 2: group_by
	gbOp, ok := plan.Pipeline[2].Op.(*mqlpb.PipeOp_GroupBy)
	if !ok {
		t.Fatalf("stage 2: expected GroupByOp, got %T", plan.Pipeline[2].Op)
	}
	if len(gbOp.GroupBy.LabelKeys) != 1 || gbOp.GroupBy.LabelKeys[0] != "metric.labels.zone" {
		t.Errorf("label_keys = %v", gbOp.GroupBy.LabelKeys)
	}
	if gbOp.GroupBy.Reducer != pb.Reducer_REDUCE_MEAN {
		t.Errorf("reducer = %v", gbOp.GroupBy.Reducer)
	}

	// Within → time_range
	if plan.TimeRange == nil {
		t.Fatal("expected time_range to be set")
	}
	rel := plan.TimeRange.GetRelative()
	if rel == nil || rel.AsDuration() != time.Hour {
		t.Errorf("time_range = %v, want 1h", plan.TimeRange)
	}
}

func TestParse_DeltaMetric(t *testing.T) {
	q := `fetch gce_instance::compute.googleapis.com/instance/disk/write_ops_count
		| align rate(60s)
		| within 30m`
	plan, err := Parse(q)
	if err != nil {
		t.Fatal(err)
	}
	alignOp := plan.Pipeline[0].GetAlign()
	if alignOp == nil {
		t.Fatal("expected align stage")
	}
	if alignOp.Aligner != pb.Aligner_ALIGN_RATE {
		t.Errorf("aligner = %v, want ALIGN_RATE", alignOp.Aligner)
	}
	if alignOp.Period.AsDuration() != 60*time.Second {
		t.Errorf("period = %v", alignOp.Period.AsDuration())
	}
}

func TestParse_LogicalPredicate(t *testing.T) {
	q := `fetch gce_instance::some/metric
		| filter metric.labels.zone = "us-central1-a" && metric.labels.env != "dev"`
	plan, err := Parse(q)
	if err != nil {
		t.Fatal(err)
	}
	filterOp := plan.Pipeline[0].GetFilter()
	if filterOp == nil {
		t.Fatal("expected filter stage")
	}
	logical := filterOp.Predicate.GetLogical()
	if logical == nil {
		t.Fatal("expected logical predicate")
	}
	if logical.Op != mqlpb.LogicalOp_AND {
		t.Errorf("logical op = %v, want AND", logical.Op)
	}
	if len(logical.Operands) != 2 {
		t.Fatalf("operands = %d, want 2", len(logical.Operands))
	}
}

func TestParse_Durations(t *testing.T) {
	cases := []struct {
		q    string
		want time.Duration
	}{
		{"fetch r::m | within 60s", 60 * time.Second},
		{"fetch r::m | within 5m", 5 * time.Minute},
		{"fetch r::m | within 2h", 2 * time.Hour},
		{"fetch r::m | within 1d", 24 * time.Hour},
		{"fetch r::m | within 1w", 7 * 24 * time.Hour},
	}
	for _, tc := range cases {
		plan, err := Parse(tc.q)
		if err != nil {
			t.Errorf("Parse(%q): %v", tc.q, err)
			continue
		}
		got := plan.TimeRange.GetRelative().AsDuration()
		if got != tc.want {
			t.Errorf("Parse(%q): duration = %v, want %v", tc.q, got, tc.want)
		}
	}
}

func TestParse_Errors(t *testing.T) {
	cases := []struct {
		q string
	}{
		{""},                        // empty
		{"align mean(1m)"},          // missing fetch
		{"fetch x | unknown_op"},    // unknown pipe op
		{"fetch gce::m | align badname(1m)"}, // unknown aligner
		{"fetch gce::m | align mean()"}, // missing duration
	}
	for _, tc := range cases {
		_, err := Parse(tc.q)
		if err == nil {
			t.Errorf("Parse(%q): expected error, got nil", tc.q)
		}
	}
}
