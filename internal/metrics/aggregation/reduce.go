package aggregation

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	pb "github.com/accretional/grpc-server-config/pb/metrics"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// reduceSeries groups input series by group_by_fields label values and applies
// the reducer across all series in each group.
// All input series must be the same kind (gauge/delta/cumulative) and aligned
// to the same period (same number of points).
func reduceSeries(series []*pb.AnyTimeSeries, reducer pb.Reducer, groupByFields []string) ([]*pb.AnyTimeSeries, error) {
	if len(series) == 0 {
		return nil, nil
	}

	// Group series by the Cartesian product of group_by_fields label values.
	type group struct {
		key    string
		series []*pb.AnyTimeSeries
	}
	ordered := []string{}
	groups := map[string]*group{}
	for _, s := range series {
		labels := seriesLabels(s)
		key := groupKey(labels, groupByFields)
		if _, ok := groups[key]; !ok {
			groups[key] = &group{key: key}
			ordered = append(ordered, key)
		}
		groups[key].series = append(groups[key].series, s)
	}

	var out []*pb.AnyTimeSeries
	for _, key := range ordered {
		g := groups[key]
		reduced, err := reduceGroup(g.series, reducer, groupByFields)
		if err != nil {
			return nil, fmt.Errorf("group %q: %w", key, err)
		}
		out = append(out, reduced)
	}
	return out, nil
}

// groupKey builds a canonical string key from the group_by_fields values of a label map.
// Empty groupByFields means all series collapse into a single group.
func groupKey(labels map[string]string, groupByFields []string) string {
	if len(groupByFields) == 0 {
		return ""
	}
	parts := make([]string, len(groupByFields))
	for i, f := range groupByFields {
		parts[i] = f + "=" + labels[f]
	}
	return strings.Join(parts, ",")
}

// seriesLabels extracts the label map from any series type.
func seriesLabels(s *pb.AnyTimeSeries) map[string]string {
	switch x := s.Series.(type) {
	case *pb.AnyTimeSeries_Gauge:
		if x.Gauge.Metric != nil {
			return x.Gauge.Metric.Labels
		}
	case *pb.AnyTimeSeries_Delta:
		if x.Delta.Metric != nil {
			return x.Delta.Metric.Labels
		}
	case *pb.AnyTimeSeries_Cumulative:
		if x.Cumulative.Metric != nil {
			return x.Cumulative.Metric.Labels
		}
	}
	return nil
}

// filteredMetric returns a copy of a Metric with only group_by_fields labels retained.
func filteredMetric(m *pb.Metric, groupByFields []string) *pb.Metric {
	if m == nil {
		return nil
	}
	out := &pb.Metric{Type: m.Type}
	if len(groupByFields) == 0 || len(m.Labels) == 0 {
		return out
	}
	keep := make(map[string]bool, len(groupByFields))
	for _, f := range groupByFields {
		keep[f] = true
	}
	out.Labels = make(map[string]string, len(groupByFields))
	for k, v := range m.Labels {
		if keep[k] {
			out.Labels[k] = v
		}
	}
	return out
}

// reduceGroup collapses all series in a group into one by applying the reducer
// at each time position. The first series in the group provides the output
// structure (timestamps, metric type, resource).
func reduceGroup(series []*pb.AnyTimeSeries, reducer pb.Reducer, groupByFields []string) (*pb.AnyTimeSeries, error) {
	if len(series) == 1 {
		return series[0], nil
	}
	switch x := series[0].Series.(type) {
	case *pb.AnyTimeSeries_Gauge:
		return reduceGaugeGroup(series, reducer, groupByFields, x.Gauge)
	case *pb.AnyTimeSeries_Delta:
		return reduceDeltaGroup(series, reducer, groupByFields, x.Delta)
	case *pb.AnyTimeSeries_Cumulative:
		return nil, fmt.Errorf("cross-series reduction of CumulativeTimeSeries is not supported; apply ALIGN_DELTA or ALIGN_RATE first")
	default:
		return nil, fmt.Errorf("unknown series type")
	}
}

// collectGaugeValuesByTimestamp groups values from all gauge series by their
// point timestamp (keyed as Unix seconds for exact matching).
func collectGaugeValuesByTimestamp(series []*pb.AnyTimeSeries) (map[int64][]float64, []int64, error) {
	buckets := make(map[int64][]float64)
	for _, s := range series {
		g, ok := s.Series.(*pb.AnyTimeSeries_Gauge)
		if !ok {
			return nil, nil, fmt.Errorf("mixed series types in reduce group")
		}
		for _, p := range g.Gauge.Points {
			tKey := p.At.GetSeconds()
			f, err := typedToFloat(p.Value)
			if err != nil {
				return nil, nil, err
			}
			buckets[tKey] = append(buckets[tKey], f)
		}
	}
	keys := make([]int64, 0, len(buckets))
	for k := range buckets {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return buckets, keys, nil
}

func reduceGaugeGroup(series []*pb.AnyTimeSeries, reducer pb.Reducer, groupByFields []string, first *pb.GaugeTimeSeries) (*pb.AnyTimeSeries, error) {
	buckets, keys, err := collectGaugeValuesByTimestamp(series)
	if err != nil {
		return nil, err
	}

	out := &pb.GaugeTimeSeries{
		Metric:   filteredMetric(first.Metric, groupByFields),
		Resource: first.Resource,
	}
	for _, tKey := range keys {
		vals := buckets[tKey]
		v, err := applyReducer(reducer, vals)
		if err != nil {
			return nil, err
		}
		out.Points = append(out.Points, &pb.GaugePoint{
			At:    timestamppb.New(time.Unix(tKey, 0)),
			Value: doubleTyped(v),
		})
	}
	return &pb.AnyTimeSeries{Series: &pb.AnyTimeSeries_Gauge{Gauge: out}}, nil
}

// reduceDeltaGroup reduces delta series point-by-point (same index = same bucket).
func reduceDeltaGroup(series []*pb.AnyTimeSeries, reducer pb.Reducer, groupByFields []string, first *pb.DeltaTimeSeries) (*pb.AnyTimeSeries, error) {
	nPoints := len(first.Points)
	for _, s := range series[1:] {
		d, ok := s.Series.(*pb.AnyTimeSeries_Delta)
		if !ok {
			return nil, fmt.Errorf("mixed series types in reduce group")
		}
		if len(d.Delta.Points) != nPoints {
			return nil, fmt.Errorf("delta series in the same group have different point counts (%d vs %d); ensure all series are aligned to the same period", nPoints, len(d.Delta.Points))
		}
	}

	out := &pb.DeltaTimeSeries{
		Metric:   filteredMetric(first.Metric, groupByFields),
		Resource: first.Resource,
		Start:    first.Start,
	}
	for i := 0; i < nPoints; i++ {
		vals := make([]float64, 0, len(series))
		for _, s := range series {
			p := s.Series.(*pb.AnyTimeSeries_Delta).Delta.Points[i]
			f, err := numericToFloat(p.Value)
			if err != nil {
				return nil, err
			}
			vals = append(vals, f)
		}
		v, err := applyReducer(reducer, vals)
		if err != nil {
			return nil, err
		}
		out.Points = append(out.Points, &pb.NumericPoint{
			Duration: first.Points[i].Duration,
			Value:    &pb.NumericValue{Value: &pb.NumericValue_DoubleValue{DoubleValue: v}},
		})
	}
	return &pb.AnyTimeSeries{Series: &pb.AnyTimeSeries_Delta{Delta: out}}, nil
}

// applyReducer applies a cross-series reducer to a slice of float64 values.
func applyReducer(reducer pb.Reducer, vals []float64) (float64, error) {
	n := len(vals)
	if n == 0 {
		return 0, nil
	}
	switch reducer {
	case pb.Reducer_REDUCE_MEAN:
		var s float64
		for _, v := range vals {
			s += v
		}
		return s / float64(n), nil
	case pb.Reducer_REDUCE_MIN:
		m := vals[0]
		for _, v := range vals[1:] {
			if v < m {
				m = v
			}
		}
		return m, nil
	case pb.Reducer_REDUCE_MAX:
		m := vals[0]
		for _, v := range vals[1:] {
			if v > m {
				m = v
			}
		}
		return m, nil
	case pb.Reducer_REDUCE_SUM:
		var s float64
		for _, v := range vals {
			s += v
		}
		return s, nil
	case pb.Reducer_REDUCE_COUNT:
		return float64(n), nil
	case pb.Reducer_REDUCE_STDDEV:
		var s float64
		for _, v := range vals {
			s += v
		}
		mean := s / float64(n)
		var ss float64
		for _, v := range vals {
			d := v - mean
			ss += d * d
		}
		return math.Sqrt(ss / float64(n)), nil
	case pb.Reducer_REDUCE_COUNT_TRUE:
		var count float64
		for _, v := range vals {
			if v != 0 {
				count++
			}
		}
		return count, nil
	case pb.Reducer_REDUCE_COUNT_FALSE:
		var count float64
		for _, v := range vals {
			if v == 0 {
				count++
			}
		}
		return count, nil
	case pb.Reducer_REDUCE_FRACTION_TRUE:
		var trueN float64
		for _, v := range vals {
			if v != 0 {
				trueN++
			}
		}
		return trueN / float64(n), nil
	case pb.Reducer_REDUCE_PERCENTILE_99:
		return percentileSlice(vals, 0.99), nil
	case pb.Reducer_REDUCE_PERCENTILE_95:
		return percentileSlice(vals, 0.95), nil
	case pb.Reducer_REDUCE_PERCENTILE_50:
		return percentileSlice(vals, 0.50), nil
	case pb.Reducer_REDUCE_PERCENTILE_05:
		return percentileSlice(vals, 0.05), nil
	}
	return 0, fmt.Errorf("unsupported reducer: %v", reducer)
}

// percentileSlice estimates a percentile from a slice using nearest-rank.
func percentileSlice(vals []float64, p float64) float64 {
	sorted := make([]float64, len(vals))
	copy(sorted, vals)
	sort.Float64s(sorted)
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
