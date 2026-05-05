package mql

import (
	"context"
	"fmt"
	"time"

	"github.com/accretional/grpc-server-config/internal/metrics/aggregation"
	pb "github.com/accretional/grpc-server-config/pb/metrics"
	mqlpb "github.com/accretional/grpc-server-config/pb/metrics"
	"google.golang.org/protobuf/types/known/durationpb"
)

// TimeSeriesReader is the storage interface the executor uses to fetch raw
// time series. Implement this to connect MQL to any data backend.
type TimeSeriesReader interface {
	// ReadTimeSeries returns all time series for the given resource and metric
	// type, within [start, end), matching the optional filter predicate.
	// A nil predicate means no filtering.
	ReadTimeSeries(
		ctx context.Context,
		resourceType, metricType string,
		start, end time.Time,
		filter *mqlpb.Predicate,
	) ([]*pb.AnyTimeSeries, error)
}

// Executor runs a QueryPlan against a TimeSeriesReader and AggregationService.
type Executor struct {
	reader  TimeSeriesReader
	agg     *aggregation.Service
}

// NewExecutor creates an Executor backed by the given reader.
func NewExecutor(reader TimeSeriesReader) *Executor {
	return &Executor{
		reader: reader,
		agg:    aggregation.New(),
	}
}

// ExecuteResult holds the output series from executing a QueryPlan.
type ExecuteResult struct {
	Series []*pb.AnyTimeSeries
}

// Execute runs the QueryPlan and returns the aligned, reduced time series.
//
// Execution order:
//  1. Resolve the time window.
//  2. Fetch raw time series via TimeSeriesReader (applies FilterOp if present).
//  3. Walk pipeline stages: AlignOp → EveryOp → GroupByOp.
func (e *Executor) Execute(ctx context.Context, plan *mqlpb.QueryPlan) (*ExecuteResult, error) {
	if plan.Fetch == nil {
		return nil, fmt.Errorf("execute: plan has no fetch operation")
	}

	// Resolve time window. Do not truncate end to the minute — interceptor
	// points are written at sub-minute precision and would be excluded otherwise.
	end := time.Now().UTC()
	start := end.Add(-time.Hour) // default: 1 hour
	if plan.TimeRange != nil {
		switch r := plan.TimeRange.Range.(type) {
		case *mqlpb.TimeRange_Relative:
			start = end.Add(-r.Relative.AsDuration())
		case *mqlpb.TimeRange_Absolute:
			start = r.Absolute.Start.AsTime()
			end = r.Absolute.End.AsTime()
		}
	}

	// Extract the filter predicate (if any) for pushdown into the reader.
	var filterPred *mqlpb.Predicate
	for _, stage := range plan.Pipeline {
		if f, ok := stage.Op.(*mqlpb.PipeOp_Filter); ok {
			filterPred = f.Filter.Predicate
			break
		}
	}

	// Fetch raw time series.
	series, err := e.reader.ReadTimeSeries(
		ctx,
		plan.Fetch.ResourceType,
		plan.Fetch.MetricType,
		start, end,
		filterPred,
	)
	if err != nil {
		return nil, fmt.Errorf("execute: fetch: %w", err)
	}

	// Execute pipeline stages.
	for _, stage := range plan.Pipeline {
		switch op := stage.Op.(type) {
		case *mqlpb.PipeOp_Filter:
			// Filter was passed to the reader; skip here to avoid double-filtering.
			// A reader that doesn't support predicate pushdown should ignore the
			// filter argument and return all series — in that case, add in-process
			// filtering here.
			_ = op

		case *mqlpb.PipeOp_Align:
			series, err = e.applyAlign(ctx, series, op.Align)
			if err != nil {
				return nil, fmt.Errorf("execute: align: %w", err)
			}

		case *mqlpb.PipeOp_Every:
			// "every" adjusts the output sampling period. When it matches the
			// alignment period it is a no-op. Downsampling (every > align) would
			// require a second alignment pass; not yet implemented.
			_ = op

		case *mqlpb.PipeOp_GroupBy:
			series, err = e.applyGroupBy(ctx, series, op.GroupBy)
			if err != nil {
				return nil, fmt.Errorf("execute: group_by: %w", err)
			}
		}
	}

	return &ExecuteResult{Series: series}, nil
}

// ---------------------------------------------------------------------------
// Align
// ---------------------------------------------------------------------------

func (e *Executor) applyAlign(
	ctx context.Context,
	series []*pb.AnyTimeSeries,
	op *mqlpb.AlignOp,
) ([]*pb.AnyTimeSeries, error) {
	period := op.Period.AsDuration()
	if period <= 0 {
		return nil, fmt.Errorf("alignment period must be positive")
	}

	out := make([]*pb.AnyTimeSeries, 0, len(series))
	for _, s := range series {
		resp, err := e.agg.Align(ctx, &pb.AlignRequest{
			Input:           s,
			Aligner:         op.Aligner,
			AlignmentPeriod: durationpb.New(period),
		})
		if err != nil {
			return nil, err
		}
		out = append(out, resp.Output)
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// GroupBy / Reduce
// ---------------------------------------------------------------------------

func (e *Executor) applyGroupBy(
	ctx context.Context,
	series []*pb.AnyTimeSeries,
	op *mqlpb.GroupByOp,
) ([]*pb.AnyTimeSeries, error) {
	// Strip the "metric.labels." / "resource.labels." prefix to get raw label
	// key names, which is what ReduceRequest.group_by_fields expects.
	labelKeys := make([]string, 0, len(op.LabelKeys))
	for _, path := range op.LabelKeys {
		key := stripLabelPrefix(path)
		labelKeys = append(labelKeys, key)
	}

	resp, err := e.agg.Reduce(ctx, &pb.ReduceRequest{
		Series:        series,
		Reducer:       op.Reducer,
		GroupByFields: labelKeys,
	})
	if err != nil {
		return nil, err
	}
	return resp.Series, nil
}

// stripLabelPrefix converts "metric.labels.zone" → "zone",
// "resource.labels.project_id" → "project_id", etc.
func stripLabelPrefix(path string) string {
	for _, prefix := range []string{
		"metric.labels.",
		"resource.labels.",
		"metadata.system_labels.",
		"metadata.user_labels.",
	} {
		if len(path) > len(prefix) && path[:len(prefix)] == prefix {
			return path[len(prefix):]
		}
	}
	return path
}
