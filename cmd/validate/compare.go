package main

import (
	"fmt"
	"math"
	"strings"
	"time"

	pb "github.com/accretional/grpc-server-config/pb/metrics"
)

// ---------------------------------------------------------------------------
// File path helper
// ---------------------------------------------------------------------------

// dataPath builds a deterministic filename for a saved GCP response.
// e.g. dataPath("data/validation", "compute.googleapis.com/instance/cpu/utilization", "raw")
//   → "data/validation/instance_cpu_utilization_raw.json"
func dataPath(dir, metricType, suffix string) string {
	name := strings.NewReplacer(
		"compute.googleapis.com/", "",
		"monitoring.googleapis.com/", "",
		"/", "_",
		".", "_",
	).Replace(metricType)
	return fmt.Sprintf("%s/%s_%s.json", dir, name, suffix)
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
			t = t.Add(p.Duration.AsDuration())
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

func gcpAlignedPoints(ts gcpTimeSeries) []timedValue {
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

func gcpValueToFloat(v gcpValue, valueType string) (float64, error) {
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

func diffPoints(ours, gcp []timedValue) (int, diffReport) {
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
// Label / series key helpers
// ---------------------------------------------------------------------------

func indexByLabels(series []gcpTimeSeries) map[string]gcpTimeSeries {
	m := make(map[string]gcpTimeSeries, len(series))
	for _, ts := range series {
		m[fullSeriesKey(&ts)] = ts
	}
	return m
}

// fullSeriesKey builds a unique lookup key from both metric and resource labels
// to prevent false matches across instances that share metric labels.
func fullSeriesKey(ts *gcpTimeSeries) string {
	combined := make(map[string]string, len(ts.Metric.Labels)+len(ts.Resource.Labels))
	for k, v := range ts.Metric.Labels {
		combined[k] = v
	}
	for k, v := range ts.Resource.Labels {
		combined["resource:"+k] = v
	}
	return labelsKey(combined)
}

func labelsKey(labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
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

// gcpReducedSeriesKey builds a lookup key for a GCP reduced series using only
// the groupByFields values.
func gcpReducedSeriesKey(ts *gcpTimeSeries, groupByFields []string) string {
	labels := make(map[string]string, len(groupByFields))
	for _, f := range groupByFields {
		if v, ok := ts.Metric.Labels[f]; ok {
			labels[f] = v
		} else if v, ok := ts.Resource.Labels[f]; ok {
			labels[f] = v
		} else {
			labels[f] = ""
		}
	}
	return labelsKey(labels)
}

// seriesOutputLabels extracts only the group_by_fields labels from an output series.
func seriesOutputLabels(s *pb.AnyTimeSeries, groupByFields []string) map[string]string {
	var allLabels map[string]string
	if g, ok := s.Series.(*pb.AnyTimeSeries_Gauge); ok && g.Gauge.Metric != nil {
		allLabels = g.Gauge.Metric.Labels
	}
	out := make(map[string]string, len(groupByFields))
	for _, f := range groupByFields {
		out[f] = allLabels[f]
	}
	return out
}

// ---------------------------------------------------------------------------
// Time range helpers
// ---------------------------------------------------------------------------

// rawTimeRange returns the earliest and latest endTime in a raw GCP series.
func rawTimeRange(ts *gcpTimeSeries) (min, max time.Time) {
	for _, p := range ts.Points {
		t, err := time.Parse(time.RFC3339, p.Interval.EndTime)
		if err != nil {
			continue
		}
		if min.IsZero() || t.Before(min) {
			min = t
		}
		if t.After(max) {
			max = t
		}
	}
	return
}

// trimGCPPoints filters aligned GCP points to the window our raw data can fully
// support, eliminating boundary mismatches caused by GCP's internal pre-history.
func trimGCPPoints(pts []timedValue, rawMin, rawMax time.Time, aligner pb.Aligner, smoothingWindow time.Duration) []timedValue {
	if rawMin.IsZero() || rawMax.IsZero() {
		return pts
	}
	from := rawMin
	if aligner == pb.Aligner_ALIGN_PERCENT_CHANGE {
		from = rawMin.Add(smoothingWindow)
	}
	out := pts[:0:0]
	for _, p := range pts {
		if !p.at.Before(from) && !p.at.After(rawMax) {
			out = append(out, p)
		}
	}
	return out
}
