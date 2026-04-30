package main

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/accretional/grpc-server-config/internal/metrics/aggregation"
	pb "github.com/accretional/grpc-server-config/pb/metrics"
	"google.golang.org/protobuf/types/known/durationpb"
)

// runComparison converts the raw GCP response to our format, runs our aligner,
// then diffs the result against the GCP-aligned response point by point.
func runComparison(
	raw, gcpAligned *GCPResponse,
	aligner pb.Aligner,
	period time.Duration,
	alignerName, metricType string,
) {
	fmt.Printf("\n══════════════════════════════════════════════════════\n")
	fmt.Printf("  %s  ·  %s  ·  period=%s\n", metricType, alignerName, period)
	fmt.Printf("══════════════════════════════════════════════════════\n\n")

	if len(raw.TimeSeries) == 0 {
		fmt.Println("  no raw time series found")
		return
	}

	svc := aggregation.New()
	totalMatched, totalPoints, totalSeries := 0, 0, 0
	var totalAlignNs int64

	// Index GCP-aligned series by a label key for O(1) lookup.
	gcpAlignedIdx := indexByLabels(gcpAligned.TimeSeries)

	for _, rawTS := range raw.TimeSeries {
		key := labelsKey(rawTS.Metric.Labels)
		gcpTS, ok := gcpAlignedIdx[key]
		if !ok {
			fmt.Printf("  [skip] no aligned series for labels %v\n", rawTS.Metric.Labels)
			continue
		}

		ourInput, err := convertTimeSeries(&rawTS)
		if err != nil {
			fmt.Printf("  [error] convert raw series %v: %v\n", rawTS.Metric.Labels, err)
			continue
		}

		t0 := time.Now()
		resp, err := svc.Align(context.Background(), &pb.AlignRequest{
			Input:           ourInput,
			Aligner:         aligner,
			AlignmentPeriod: durationpb.New(period),
		})
		alignDur := time.Since(t0)
		totalAlignNs += alignDur.Nanoseconds()

		if err != nil {
			fmt.Printf("  [error] align %v: %v\n", rawTS.Metric.Labels, err)
			continue
		}

		ourPoints := extractPoints(resp.Output)
		gcpPoints := gcpAlignedPoints(gcpTS)

		matched, report := diffPoints(ourPoints, gcpPoints)
		totalMatched += matched
		totalPoints += len(gcpPoints)
		totalSeries++

		fmt.Printf("  Series: %v\n", rawTS.Metric.Labels)
		fmt.Printf("    raw points:     %d\n", len(rawTS.Points))
		fmt.Printf("    GCP output:     %d points\n", len(gcpPoints))
		fmt.Printf("    our output:     %d points\n", len(ourPoints))
		fmt.Printf("    matched:        %d/%d\n", matched, len(gcpPoints))
		fmt.Printf("    align latency:  %s\n", alignDur.Round(time.Microsecond))
		if report.maxAbsDiff > 0 || report.countMismatch > 0 {
			fmt.Printf("    max abs diff:   %g\n", report.maxAbsDiff)
			fmt.Printf("    max rel diff:   %g%%\n", report.maxRelDiff*100)
			fmt.Printf("    mismatched:     %d\n", report.countMismatch)
			for _, m := range report.examples {
				fmt.Printf("    ↳ %s  GCP=%-18g  ours=%-18g  Δ=%g\n",
					m.at.Format("15:04:05"), m.gcp, m.ours, m.absDiff)
			}
		} else {
			fmt.Printf("    ✓ exact match\n")
		}
		fmt.Println()
	}

	avgAlignUs := int64(0)
	if totalSeries > 0 {
		avgAlignUs = totalAlignNs / int64(totalSeries) / 1000
	}
	fmt.Printf("──────────────────────────────────────────────────────\n")
	fmt.Printf("  Total series: %d  |  Points matched: %d/%d\n",
		totalSeries, totalMatched, totalPoints)
	fmt.Printf("  Our align latency: avg=%dµs  total=%s\n\n",
		avgAlignUs, time.Duration(totalAlignNs).Round(time.Microsecond))
}

// ---------------------------------------------------------------------------
// Point extraction
// ---------------------------------------------------------------------------

type timedValue struct {
	at  time.Time
	val float64
}

func extractPoints(s *pb.AnyTimeSeries) []timedValue {
	switch x := s.Series.(type) {
	case *pb.AnyTimeSeries_Gauge:
		out := make([]timedValue, 0, len(x.Gauge.Points))
		for _, p := range x.Gauge.Points {
			f, err := typedValueToFloat(p.Value)
			if err != nil {
				continue
			}
			out = append(out, timedValue{at: p.At.AsTime(), val: f})
		}
		return out
	case *pb.AnyTimeSeries_Delta:
		out := make([]timedValue, 0, len(x.Delta.Points))
		t := x.Delta.Start.AsTime()
		for _, p := range x.Delta.Points {
			d := p.Duration.AsDuration()
			t = t.Add(d)
			f, err := numericValueToFloat(p.Value)
			if err != nil {
				continue
			}
			out = append(out, timedValue{at: t, val: f})
		}
		return out
	}
	return nil
}

func gcpAlignedPoints(ts GCPTimeSeries) []timedValue {
	out := make([]timedValue, 0, len(ts.Points))
	for _, p := range ts.Points {
		t, err := time.Parse(time.RFC3339, p.Interval.EndTime)
		if err != nil {
			continue
		}
		f, err := gcpValueToFloat(p.Value, ts.ValueType)
		if err != nil {
			continue
		}
		out = append(out, timedValue{at: t, val: f})
	}
	return out
}

func gcpValueToFloat(v GCPValue, valueType string) (float64, error) {
	switch valueType {
	case "DOUBLE":
		if v.DoubleValue != nil {
			return *v.DoubleValue, nil
		}
	case "INT64":
		if v.Int64Value != nil {
			var n int64
			_, err := fmt.Sscan(*v.Int64Value, &n)
			return float64(n), err
		}
	}
	return 0, fmt.Errorf("no value")
}

func typedValueToFloat(v *pb.TypedValue) (float64, error) {
	switch x := v.Value.(type) {
	case *pb.TypedValue_DoubleValue:
		return x.DoubleValue, nil
	case *pb.TypedValue_Int64Value:
		return float64(x.Int64Value), nil
	}
	return 0, fmt.Errorf("non-numeric typed value")
}

func numericValueToFloat(v *pb.NumericValue) (float64, error) {
	switch x := v.Value.(type) {
	case *pb.NumericValue_DoubleValue:
		return x.DoubleValue, nil
	case *pb.NumericValue_Int64Value:
		return float64(x.Int64Value), nil
	}
	return 0, fmt.Errorf("distribution value")
}

// ---------------------------------------------------------------------------
// Diff
// ---------------------------------------------------------------------------

const (
	absTolerance = 1e-9
	relTolerance = 1e-6
	maxExamples  = 5
)

type diffReport struct {
	maxAbsDiff    float64
	maxRelDiff    float64
	countMismatch int
	examples      []mismatchExample
}

type mismatchExample struct {
	at      time.Time
	gcp     float64
	ours    float64
	absDiff float64
}

// diffPoints matches our output to GCP's output by timestamp and computes diffs.
// Returns the count of matched (within tolerance) points and a report.
func diffPoints(ours, gcp []timedValue) (int, diffReport) {
	// Index ours by Unix second for O(1) lookup.
	ourIdx := make(map[int64]float64, len(ours))
	for _, p := range ours {
		ourIdx[p.at.Unix()] = p.val
	}

	var r diffReport
	matched := 0
	for _, g := range gcp {
		ov, ok := ourIdx[g.at.Unix()]
		if !ok {
			r.countMismatch++
			if len(r.examples) < maxExamples {
				r.examples = append(r.examples, mismatchExample{at: g.at, gcp: g.val, ours: math.NaN()})
			}
			continue
		}
		absDiff := math.Abs(ov - g.val)
		relDiff := 0.0
		if g.val != 0 {
			relDiff = absDiff / math.Abs(g.val)
		}
		if r.maxAbsDiff < absDiff {
			r.maxAbsDiff = absDiff
		}
		if r.maxRelDiff < relDiff {
			r.maxRelDiff = relDiff
		}
		if absDiff > absTolerance && relDiff > relTolerance {
			r.countMismatch++
			if len(r.examples) < maxExamples {
				r.examples = append(r.examples, mismatchExample{at: g.at, gcp: g.val, ours: ov, absDiff: absDiff})
			}
		} else {
			matched++
		}
	}
	return matched, r
}

// ---------------------------------------------------------------------------
// Label helpers
// ---------------------------------------------------------------------------

func indexByLabels(series []GCPTimeSeries) map[string]GCPTimeSeries {
	m := make(map[string]GCPTimeSeries, len(series))
	for _, ts := range series {
		m[labelsKey(ts.Metric.Labels)] = ts
	}
	return m
}

func labelsKey(labels map[string]string) string {
	// Stable key from all label values sorted by key.
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	// Simple sort inline to avoid importing sort.
	for i := range keys {
		for j := i + 1; j < len(keys); j++ {
			if keys[i] > keys[j] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = k + "=" + labels[k]
	}
	return fmt.Sprintf("%v", parts)
}
