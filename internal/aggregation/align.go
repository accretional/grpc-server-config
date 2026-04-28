package aggregation

import (
	"fmt"
	"math"
	"sort"
	"time"

	pb "github.com/accretional/grpc-server-config/pb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func alignSeries(input *pb.AnyTimeSeries, aligner pb.Aligner, period time.Duration) (*pb.AnyTimeSeries, error) {
	switch s := input.Series.(type) {
	case *pb.AnyTimeSeries_Gauge:
		return alignGauge(s.Gauge, aligner, period)
	case *pb.AnyTimeSeries_Delta:
		return alignDelta(s.Delta, aligner, period)
	case *pb.AnyTimeSeries_Cumulative:
		return alignCumulative(s.Cumulative, aligner, period)
	default:
		return nil, fmt.Errorf("unknown series type")
	}
}

// ---------------------------------------------------------------------------
// Bucket helpers
// ---------------------------------------------------------------------------

// bucketIdx returns the epoch-aligned bucket index for a Unix time in seconds.
// Uses right-closed intervals (T-ps, T]: a point exactly on a boundary belongs
// to the bucket that ends there, not the one that starts there.
// This matches GCP Cloud Monitoring's convention for GAUGE alignment.
//
// For non-boundary T: equivalent to floor(T/ps).
// For boundary T (T divisible by ps): returns T/ps - 1 so output timestamp == T.
func bucketIdx(tSecs, ps float64) int64 {
	return int64(math.Ceil(tSecs/ps)) - 1
}

// bucketEndTS returns a Timestamp at the end of bucket i.
func bucketEndTS(i int64, ps float64) *timestamppb.Timestamp {
	return timestamppb.New(time.Unix(int64(float64(i+1)*ps), 0))
}

// sortedKeys returns the keys of a map[int64]T in ascending order.
func sortedKeys[T any](m map[int64]T) []int64 {
	keys := make([]int64, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

// ---------------------------------------------------------------------------
// Value helpers
// ---------------------------------------------------------------------------

func typedToFloat(v *pb.TypedValue) (float64, error) {
	switch x := v.Value.(type) {
	case *pb.TypedValue_Int64Value:
		return float64(x.Int64Value), nil
	case *pb.TypedValue_DoubleValue:
		return x.DoubleValue, nil
	default:
		return 0, fmt.Errorf("value is not numeric (bool/string/distribution)")
	}
}

func numericToFloat(v *pb.NumericValue) (float64, error) {
	switch x := v.Value.(type) {
	case *pb.NumericValue_Int64Value:
		return float64(x.Int64Value), nil
	case *pb.NumericValue_DoubleValue:
		return x.DoubleValue, nil
	default:
		return 0, fmt.Errorf("distribution cannot be reduced to a single float")
	}
}

func doubleTyped(f float64) *pb.TypedValue {
	return &pb.TypedValue{Value: &pb.TypedValue_DoubleValue{DoubleValue: f}}
}

// ---------------------------------------------------------------------------
// Statistical aggregation
// ---------------------------------------------------------------------------

// applyStat applies a statistical aligner to a slice of float64 values.
func applyStat(aligner pb.Aligner, vals []float64) (float64, error) {
	n := len(vals)
	if n == 0 {
		return 0, nil
	}
	switch aligner {
	case pb.Aligner_ALIGN_MIN:
		m := vals[0]
		for _, v := range vals[1:] {
			if v < m {
				m = v
			}
		}
		return m, nil
	case pb.Aligner_ALIGN_MAX:
		m := vals[0]
		for _, v := range vals[1:] {
			if v > m {
				m = v
			}
		}
		return m, nil
	case pb.Aligner_ALIGN_SUM:
		var s float64
		for _, v := range vals {
			s += v
		}
		return s, nil
	case pb.Aligner_ALIGN_COUNT:
		return float64(n), nil
	case pb.Aligner_ALIGN_MEAN:
		var s float64
		for _, v := range vals {
			s += v
		}
		return s / float64(n), nil
	case pb.Aligner_ALIGN_STDDEV:
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
	}
	return 0, fmt.Errorf("unsupported statistical aligner: %v", aligner)
}

// ---------------------------------------------------------------------------
// Gauge align
// ---------------------------------------------------------------------------

func alignGauge(s *pb.GaugeTimeSeries, aligner pb.Aligner, period time.Duration) (*pb.AnyTimeSeries, error) {
	ps := period.Seconds()
	switch aligner {
	case pb.Aligner_ALIGN_COUNT_TRUE, pb.Aligner_ALIGN_COUNT_FALSE, pb.Aligner_ALIGN_FRACTION_TRUE:
		return alignGaugeBool(s, aligner, ps)
	case pb.Aligner_ALIGN_INTERPOLATE:
		return alignGaugeInterpolate(s, ps)
	case pb.Aligner_ALIGN_NEXT_OLDER:
		return alignGaugeNextOlder(s, ps)
	case pb.Aligner_ALIGN_PERCENT_CHANGE:
		return alignGaugePercentChange(s, ps)
	case pb.Aligner_ALIGN_PERCENTILE_99:
		return alignGaugePercentile(s, 0.99, ps)
	case pb.Aligner_ALIGN_PERCENTILE_95:
		return alignGaugePercentile(s, 0.95, ps)
	case pb.Aligner_ALIGN_PERCENTILE_50:
		return alignGaugePercentile(s, 0.50, ps)
	case pb.Aligner_ALIGN_PERCENTILE_05:
		return alignGaugePercentile(s, 0.05, ps)
	default:
		return alignGaugeStat(s, aligner, ps)
	}
}

// alignGaugeStat handles MIN, MAX, MEAN, SUM, COUNT, STDDEV.
func alignGaugeStat(s *pb.GaugeTimeSeries, aligner pb.Aligner, ps float64) (*pb.AnyTimeSeries, error) {
	buckets := make(map[int64][]float64)
	for _, p := range s.Points {
		idx := bucketIdx(float64(p.At.GetSeconds()), ps)
		f, err := typedToFloat(p.Value)
		if err != nil {
			return nil, err
		}
		buckets[idx] = append(buckets[idx], f)
	}
	out := &pb.GaugeTimeSeries{Metric: s.Metric, Resource: s.Resource}
	for _, idx := range sortedKeys(buckets) {
		v, err := applyStat(aligner, buckets[idx])
		if err != nil {
			return nil, err
		}
		out.Points = append(out.Points, &pb.GaugePoint{At: bucketEndTS(idx, ps), Value: doubleTyped(v)})
	}
	return &pb.AnyTimeSeries{Series: &pb.AnyTimeSeries_Gauge{Gauge: out}}, nil
}

// alignGaugeBool handles COUNT_TRUE, COUNT_FALSE, FRACTION_TRUE.
func alignGaugeBool(s *pb.GaugeTimeSeries, aligner pb.Aligner, ps float64) (*pb.AnyTimeSeries, error) {
	type counts struct{ trueN, totalN int64 }
	buckets := make(map[int64]*counts)
	for _, p := range s.Points {
		idx := bucketIdx(float64(p.At.GetSeconds()), ps)
		if buckets[idx] == nil {
			buckets[idx] = &counts{}
		}
		buckets[idx].totalN++
		if bv, ok := p.Value.Value.(*pb.TypedValue_BoolValue); ok && bv.BoolValue {
			buckets[idx].trueN++
		}
	}
	out := &pb.GaugeTimeSeries{Metric: s.Metric, Resource: s.Resource}
	for _, idx := range sortedKeys(buckets) {
		c := buckets[idx]
		var tv *pb.TypedValue
		switch aligner {
		case pb.Aligner_ALIGN_COUNT_TRUE:
			tv = &pb.TypedValue{Value: &pb.TypedValue_Int64Value{Int64Value: c.trueN}}
		case pb.Aligner_ALIGN_COUNT_FALSE:
			tv = &pb.TypedValue{Value: &pb.TypedValue_Int64Value{Int64Value: c.totalN - c.trueN}}
		case pb.Aligner_ALIGN_FRACTION_TRUE:
			frac := 0.0
			if c.totalN > 0 {
				frac = float64(c.trueN) / float64(c.totalN)
			}
			tv = doubleTyped(frac)
		}
		out.Points = append(out.Points, &pb.GaugePoint{At: bucketEndTS(idx, ps), Value: tv})
	}
	return &pb.AnyTimeSeries{Series: &pb.AnyTimeSeries_Gauge{Gauge: out}}, nil
}

// alignGaugeInterpolate linearly interpolates gauge values at each bucket boundary.
// Follows GCP's convention: extends one bucket past the last raw point using linear
// extrapolation from the last two raw points.
func alignGaugeInterpolate(s *pb.GaugeTimeSeries, ps float64) (*pb.AnyTimeSeries, error) {
	if len(s.Points) < 2 {
		return &pb.AnyTimeSeries{Series: &pb.AnyTimeSeries_Gauge{Gauge: s}}, nil
	}
	firstIdx := bucketIdx(float64(s.Points[0].At.GetSeconds()), ps)
	// GCP extends one extrapolated bucket past the last raw point.
	lastIdx := bucketIdx(float64(s.Points[len(s.Points)-1].At.GetSeconds()), ps) + 1
	out := &pb.GaugeTimeSeries{Metric: s.Metric, Resource: s.Resource}
	for idx := firstIdx; idx <= lastIdx; idx++ {
		tSecs := float64(idx+1) * ps
		v, err := interpolateGaugeAt(s.Points, tSecs)
		if err != nil {
			continue
		}
		out.Points = append(out.Points, &pb.GaugePoint{
			At:    timestamppb.New(time.Unix(int64(tSecs), 0)),
			Value: doubleTyped(v),
		})
	}
	return &pb.AnyTimeSeries{Series: &pb.AnyTimeSeries_Gauge{Gauge: out}}, nil
}

func interpolateGaugeAt(pts []*pb.GaugePoint, tSecs float64) (float64, error) {
	n := len(pts)
	for i := 0; i < n-1; i++ {
		t0 := float64(pts[i].At.GetSeconds())
		t1 := float64(pts[i+1].At.GetSeconds())
		if tSecs < t0 || tSecs > t1 {
			continue
		}
		v0, err := typedToFloat(pts[i].Value)
		if err != nil {
			return 0, err
		}
		v1, err := typedToFloat(pts[i+1].Value)
		if err != nil {
			return 0, err
		}
		if t1 == t0 {
			return v0, nil
		}
		return v0 + (v1-v0)*(tSecs-t0)/(t1-t0), nil
	}
	// Linear extrapolation beyond the last raw point using the last two points.
	if tSecs > float64(pts[n-1].At.GetSeconds()) && n >= 2 {
		t0 := float64(pts[n-2].At.GetSeconds())
		t1 := float64(pts[n-1].At.GetSeconds())
		v0, err := typedToFloat(pts[n-2].Value)
		if err != nil {
			return 0, err
		}
		v1, err := typedToFloat(pts[n-1].Value)
		if err != nil {
			return 0, err
		}
		if t1 == t0 {
			return v1, nil
		}
		return v1 + (v1-v0)*(tSecs-t1)/(t1-t0), nil
	}
	return 0, fmt.Errorf("t=%v is outside the range of points", tSecs)
}

// alignGaugeNextOlder carries the most recent value forward to each bucket boundary.
// Follows GCP's convention: a raw point exactly on a boundary (T) belongs to the
// bucket ending at T (inclusive ≤), and the last raw value is carried one extra
// bucket beyond the series end.
func alignGaugeNextOlder(s *pb.GaugeTimeSeries, ps float64) (*pb.AnyTimeSeries, error) {
	if len(s.Points) == 0 {
		return &pb.AnyTimeSeries{Series: &pb.AnyTimeSeries_Gauge{Gauge: s}}, nil
	}
	firstIdx := bucketIdx(float64(s.Points[0].At.GetSeconds()), ps)
	// GCP extends one carry-forward bucket past the last raw point.
	lastIdx := bucketIdx(float64(s.Points[len(s.Points)-1].At.GetSeconds()), ps) + 1
	out := &pb.GaugeTimeSeries{Metric: s.Metric, Resource: s.Resource}
	ptIdx := 0
	var lastVal *pb.TypedValue
	for idx := firstIdx; idx <= lastIdx; idx++ {
		bucketEnd := float64(idx+1) * ps
		// Use <= so a raw point exactly at the bucket boundary belongs to this bucket.
		for ptIdx < len(s.Points) && float64(s.Points[ptIdx].At.GetSeconds()) <= bucketEnd {
			lastVal = s.Points[ptIdx].Value
			ptIdx++
		}
		if lastVal != nil {
			out.Points = append(out.Points, &pb.GaugePoint{
				At:    timestamppb.New(time.Unix(int64(bucketEnd), 0)),
				Value: lastVal,
			})
		}
	}
	return &pb.AnyTimeSeries{Series: &pb.AnyTimeSeries_Gauge{Gauge: out}}, nil
}

// alignGaugePercentChange computes the % change between consecutive bucket means.
// ((current - previous) / |previous|) * 100.
func alignGaugePercentChange(s *pb.GaugeTimeSeries, ps float64) (*pb.AnyTimeSeries, error) {
	buckets := make(map[int64][]float64)
	for _, p := range s.Points {
		idx := bucketIdx(float64(p.At.GetSeconds()), ps)
		f, err := typedToFloat(p.Value)
		if err != nil {
			return nil, err
		}
		buckets[idx] = append(buckets[idx], f)
	}
	keys := sortedKeys(buckets)
	means := make([]float64, len(keys))
	for i, idx := range keys {
		m, _ := applyStat(pb.Aligner_ALIGN_MEAN, buckets[idx])
		means[i] = m
	}
	out := &pb.GaugeTimeSeries{Metric: s.Metric, Resource: s.Resource}
	for i := 1; i < len(keys); i++ {
		prev, curr := means[i-1], means[i]
		var pct float64
		if prev != 0 {
			pct = ((curr - prev) / math.Abs(prev)) * 100
		}
		out.Points = append(out.Points, &pb.GaugePoint{At: bucketEndTS(keys[i], ps), Value: doubleTyped(pct)})
	}
	return &pb.AnyTimeSeries{Series: &pb.AnyTimeSeries_Gauge{Gauge: out}}, nil
}

// alignGaugePercentile estimates a percentile from Distribution-valued gauge points.
func alignGaugePercentile(s *pb.GaugeTimeSeries, pct, ps float64) (*pb.AnyTimeSeries, error) {
	buckets := make(map[int64]*pb.Distribution)
	for _, p := range s.Points {
		idx := bucketIdx(float64(p.At.GetSeconds()), ps)
		dv, ok := p.Value.Value.(*pb.TypedValue_DistributionValue)
		if !ok {
			return nil, fmt.Errorf("ALIGN_PERCENTILE_* requires DISTRIBUTION values")
		}
		if buckets[idx] == nil {
			buckets[idx] = dv.DistributionValue
		} else {
			merged, err := mergeDistributions(buckets[idx], dv.DistributionValue)
			if err != nil {
				return nil, err
			}
			buckets[idx] = merged
		}
	}
	out := &pb.GaugeTimeSeries{Metric: s.Metric, Resource: s.Resource}
	for _, idx := range sortedKeys(buckets) {
		v, err := distributionPercentile(buckets[idx], pct)
		if err != nil {
			return nil, err
		}
		out.Points = append(out.Points, &pb.GaugePoint{At: bucketEndTS(idx, ps), Value: doubleTyped(v)})
	}
	return &pb.AnyTimeSeries{Series: &pb.AnyTimeSeries_Gauge{Gauge: out}}, nil
}

// ---------------------------------------------------------------------------
// Delta align
// ---------------------------------------------------------------------------

// deltaWindowTimes reconstructs the absolute [start, end) for each NumericPoint.
func deltaWindowTimes(s *pb.DeltaTimeSeries) (starts, ends []float64) {
	t := float64(s.Start.GetSeconds())
	for _, p := range s.Points {
		d := p.Duration.AsDuration().Seconds()
		starts = append(starts, t)
		ends = append(ends, t+d)
		t += d
	}
	return
}

func alignDelta(s *pb.DeltaTimeSeries, aligner pb.Aligner, period time.Duration) (*pb.AnyTimeSeries, error) {
	if len(s.Points) == 0 {
		return &pb.AnyTimeSeries{Series: &pb.AnyTimeSeries_Delta{Delta: s}}, nil
	}
	ps := period.Seconds()
	starts, ends := deltaWindowTimes(s)
	switch aligner {
	case pb.Aligner_ALIGN_RATE:
		return alignDeltaRate(s, starts, ends, ps)
	case pb.Aligner_ALIGN_DELTA:
		// Re-bucket: sum raw windows into alignment-period windows, keep DELTA structure.
		return alignDeltaRebucket(s, starts, ends, ps, period)
	default:
		return alignDeltaStat(s, aligner, starts, ends, ps)
	}
}

// alignDeltaRebucket sums raw delta windows into alignment-period buckets and
// returns a DeltaTimeSeries (ALIGN_DELTA semantics: delta-in → delta-out).
// Windows spanning bucket boundaries are split proportionally by time.
func alignDeltaRebucket(s *pb.DeltaTimeSeries, starts, ends []float64, ps float64, period time.Duration) (*pb.AnyTimeSeries, error) {
	bucketSums := make(map[int64]float64)
	for i, p := range s.Points {
		ws, we, wd := starts[i], ends[i], ends[i]-starts[i]
		v, err := numericToFloat(p.Value)
		if err != nil {
			return nil, err
		}
		startB := bucketIdx(ws, ps)
		endB := bucketIdx(we-1e-9, ps)
		for b := startB; b <= endB; b++ {
			overlapStart := math.Max(ws, float64(b)*ps)
			overlapEnd := math.Min(we, float64(b+1)*ps)
			bucketSums[b] += v * (overlapEnd-overlapStart) / wd
		}
	}
	keys := sortedKeys(bucketSums)
	if len(keys) == 0 {
		return &pb.AnyTimeSeries{Series: &pb.AnyTimeSeries_Delta{Delta: s}}, nil
	}
	out := &pb.DeltaTimeSeries{
		Metric:   s.Metric,
		Resource: s.Resource,
		Start:    timestamppb.New(time.Unix(int64(float64(keys[0])*ps), 0)),
	}
	for _, idx := range keys {
		out.Points = append(out.Points, &pb.NumericPoint{
			Duration: durationpb.New(period),
			Value:    &pb.NumericValue{Value: &pb.NumericValue_DoubleValue{DoubleValue: bucketSums[idx]}},
		})
	}
	return &pb.AnyTimeSeries{Series: &pb.AnyTimeSeries_Delta{Delta: out}}, nil
}

// alignDeltaRate computes value/second for each alignment bucket.
// Windows spanning bucket boundaries are split proportionally by time.
func alignDeltaRate(s *pb.DeltaTimeSeries, starts, ends []float64, ps float64) (*pb.AnyTimeSeries, error) {
	bucketSums := make(map[int64]float64)
	for i, p := range s.Points {
		ws, we, wd := starts[i], ends[i], ends[i]-starts[i]
		v, err := numericToFloat(p.Value)
		if err != nil {
			return nil, err
		}
		startB := bucketIdx(ws, ps)
		// Subtract a tiny epsilon so a point ending exactly on a boundary stays in the previous bucket.
		endB := bucketIdx(we-1e-9, ps)
		for b := startB; b <= endB; b++ {
			overlapStart := math.Max(ws, float64(b)*ps)
			overlapEnd := math.Min(we, float64(b+1)*ps)
			bucketSums[b] += v * (overlapEnd-overlapStart) / wd
		}
	}
	out := &pb.GaugeTimeSeries{Metric: s.Metric, Resource: s.Resource}
	for _, idx := range sortedKeys(bucketSums) {
		out.Points = append(out.Points, &pb.GaugePoint{
			At:    bucketEndTS(idx, ps),
			Value: doubleTyped(bucketSums[idx] / ps),
		})
	}
	return &pb.AnyTimeSeries{Series: &pb.AnyTimeSeries_Gauge{Gauge: out}}, nil
}

// alignDeltaStat assigns each delta window to the bucket containing its midpoint,
// then applies a statistical aggregation over all windows in that bucket.
func alignDeltaStat(s *pb.DeltaTimeSeries, aligner pb.Aligner, starts, ends []float64, ps float64) (*pb.AnyTimeSeries, error) {
	buckets := make(map[int64][]float64)
	for i, p := range s.Points {
		idx := bucketIdx((starts[i]+ends[i])/2, ps)
		v, err := numericToFloat(p.Value)
		if err != nil {
			return nil, err
		}
		buckets[idx] = append(buckets[idx], v)
	}
	out := &pb.GaugeTimeSeries{Metric: s.Metric, Resource: s.Resource}
	for _, idx := range sortedKeys(buckets) {
		v, err := applyStat(aligner, buckets[idx])
		if err != nil {
			return nil, err
		}
		out.Points = append(out.Points, &pb.GaugePoint{At: bucketEndTS(idx, ps), Value: doubleTyped(v)})
	}
	return &pb.AnyTimeSeries{Series: &pb.AnyTimeSeries_Gauge{Gauge: out}}, nil
}

// ---------------------------------------------------------------------------
// Cumulative align
// ---------------------------------------------------------------------------

// cumulativeObservations returns (absolute_end_time_secs, value) for each point.
func cumulativeObservations(s *pb.CumulativeTimeSeries) (times []float64, values []*pb.NumericValue) {
	t := float64(s.EpochStart.GetSeconds())
	for _, p := range s.Points {
		t += p.Duration.AsDuration().Seconds()
		times = append(times, t)
		values = append(values, p.Value)
	}
	return
}

func alignCumulative(s *pb.CumulativeTimeSeries, aligner pb.Aligner, period time.Duration) (*pb.AnyTimeSeries, error) {
	if len(s.Points) == 0 {
		return &pb.AnyTimeSeries{Series: &pb.AnyTimeSeries_Cumulative{Cumulative: s}}, nil
	}
	ps := period.Seconds()
	switch aligner {
	case pb.Aligner_ALIGN_DELTA:
		return alignCumulativeDelta(s, ps, period)
	case pb.Aligner_ALIGN_RATE:
		return alignCumulativeRate(s, ps, period)
	default:
		return nil, fmt.Errorf("aligner %v is not valid for CumulativeTimeSeries (use ALIGN_DELTA or ALIGN_RATE)", aligner)
	}
}

// interpolateCumulativeAt estimates the cumulative value at tSecs.
// Before the first observation it interpolates linearly from 0 at epochStart.
// After the last observation it returns the last known value.
func interpolateCumulativeAt(times []float64, values []*pb.NumericValue, epochStart, tSecs float64) (float64, error) {
	if tSecs <= epochStart {
		return 0, nil
	}
	if tSecs >= times[len(times)-1] {
		return numericToFloat(values[len(values)-1])
	}
	// Before first point: interpolate from (epochStart, 0) to (times[0], values[0])
	if tSecs < times[0] {
		v0, err := numericToFloat(values[0])
		if err != nil {
			return 0, err
		}
		return v0 * (tSecs - epochStart) / (times[0] - epochStart), nil
	}
	for i := 0; i < len(times)-1; i++ {
		if tSecs >= times[i] && tSecs <= times[i+1] {
			v0, err := numericToFloat(values[i])
			if err != nil {
				return 0, err
			}
			v1, err := numericToFloat(values[i+1])
			if err != nil {
				return 0, err
			}
			if times[i+1] == times[i] {
				return v0, nil
			}
			return v0 + (v1-v0)*(tSecs-times[i])/(times[i+1]-times[i]), nil
		}
	}
	return 0, fmt.Errorf("interpolation failed at t=%v", tSecs)
}

func alignCumulativeDelta(s *pb.CumulativeTimeSeries, ps float64, period time.Duration) (*pb.AnyTimeSeries, error) {
	epochStart := float64(s.EpochStart.GetSeconds())
	times, values := cumulativeObservations(s)
	firstBucket := bucketIdx(epochStart, ps)
	lastBucket := bucketIdx(times[len(times)-1], ps)

	out := &pb.DeltaTimeSeries{
		Metric:   s.Metric,
		Resource: s.Resource,
		Start:    timestamppb.New(time.Unix(int64(float64(firstBucket)*ps), 0)),
	}
	for b := firstBucket; b < lastBucket; b++ {
		bStart := float64(b) * ps
		bEnd := float64(b+1) * ps
		vStart, err := interpolateCumulativeAt(times, values, epochStart, bStart)
		if err != nil {
			return nil, err
		}
		vEnd, err := interpolateCumulativeAt(times, values, epochStart, bEnd)
		if err != nil {
			return nil, err
		}
		out.Points = append(out.Points, &pb.NumericPoint{
			Duration: durationpb.New(period),
			Value:    &pb.NumericValue{Value: &pb.NumericValue_DoubleValue{DoubleValue: vEnd - vStart}},
		})
	}
	return &pb.AnyTimeSeries{Series: &pb.AnyTimeSeries_Delta{Delta: out}}, nil
}

func alignCumulativeRate(s *pb.CumulativeTimeSeries, ps float64, period time.Duration) (*pb.AnyTimeSeries, error) {
	epochStart := float64(s.EpochStart.GetSeconds())
	times, values := cumulativeObservations(s)
	firstBucket := bucketIdx(epochStart, ps)
	lastBucket := bucketIdx(times[len(times)-1], ps)

	out := &pb.GaugeTimeSeries{Metric: s.Metric, Resource: s.Resource}
	for b := firstBucket; b < lastBucket; b++ {
		bStart := float64(b) * ps
		bEnd := float64(b+1) * ps
		vStart, err := interpolateCumulativeAt(times, values, epochStart, bStart)
		if err != nil {
			return nil, err
		}
		vEnd, err := interpolateCumulativeAt(times, values, epochStart, bEnd)
		if err != nil {
			return nil, err
		}
		out.Points = append(out.Points, &pb.GaugePoint{
			At:    bucketEndTS(b, ps),
			Value: doubleTyped((vEnd - vStart) / ps),
		})
	}
	return &pb.AnyTimeSeries{Series: &pb.AnyTimeSeries_Gauge{Gauge: out}}, nil
}

// ---------------------------------------------------------------------------
// Distribution helpers
// ---------------------------------------------------------------------------

// mergeDistributions combines two Distribution values (e.g. two points in the
// same alignment bucket) using the parallel variance formula.
func mergeDistributions(a, b *pb.Distribution) (*pb.Distribution, error) {
	if a.BucketOptions == nil || b.BucketOptions == nil {
		return nil, fmt.Errorf("cannot merge distributions without bucket options")
	}
	if len(a.BucketCounts) != len(b.BucketCounts) {
		return nil, fmt.Errorf("cannot merge distributions with different bucket counts (%d vs %d)",
			len(a.BucketCounts), len(b.BucketCounts))
	}
	na, nb := float64(a.Count), float64(b.Count)
	nc := na + nb
	var combinedMean, combinedSSD float64
	if nc > 0 {
		combinedMean = (na*a.Mean + nb*b.Mean) / nc
		combinedSSD = a.SumOfSquaredDeviation + b.SumOfSquaredDeviation +
			(na*nb/nc)*(a.Mean-b.Mean)*(a.Mean-b.Mean)
	}
	buckets := make([]int64, len(a.BucketCounts))
	for i := range buckets {
		buckets[i] = a.BucketCounts[i] + b.BucketCounts[i]
	}
	merged := &pb.Distribution{
		Count:                 a.Count + b.Count,
		Mean:                  combinedMean,
		SumOfSquaredDeviation: combinedSSD,
		BucketOptions:         a.BucketOptions,
		BucketCounts:          buckets,
	}
	if a.Range != nil && b.Range != nil {
		merged.Range = &pb.Distribution_Range{
			Min: math.Min(a.Range.Min, b.Range.Min),
			Max: math.Max(a.Range.Max, b.Range.Max),
		}
	}
	return merged, nil
}

// distributionPercentile estimates the p-th percentile (0..1) from a Distribution.
// Uses linear interpolation assuming uniform distribution within each bucket.
func distributionPercentile(d *pb.Distribution, p float64) (float64, error) {
	if d.Count == 0 {
		return 0, nil
	}
	target := p * float64(d.Count)
	var cumulative float64
	for i, count := range d.BucketCounts {
		cumulative += float64(count)
		if cumulative >= target {
			lower, upper, err := bucketBounds(d, i)
			if err != nil {
				return 0, err
			}
			// Clamp infinities using observed range when available.
			if math.IsInf(lower, -1) {
				if d.Range != nil {
					lower = d.Range.Min
				} else {
					lower = upper - 1
				}
			}
			if math.IsInf(upper, 1) {
				if d.Range != nil {
					upper = d.Range.Max
				} else {
					upper = lower + 1
				}
			}
			prev := cumulative - float64(count)
			frac := 0.0
			if count > 0 {
				frac = (target - prev) / float64(count)
			}
			return lower + frac*(upper-lower), nil
		}
	}
	if d.Range != nil {
		return d.Range.Max, nil
	}
	return 0, fmt.Errorf("could not compute percentile")
}

func bucketBounds(d *pb.Distribution, i int) (lower, upper float64, err error) {
	if d.BucketOptions == nil {
		return 0, 0, fmt.Errorf("no bucket options")
	}
	switch opts := d.BucketOptions.Options.(type) {
	case *pb.Distribution_BucketOptions_ExponentialBuckets:
		e := opts.ExponentialBuckets
		n := int(e.NumFiniteBuckets)
		switch {
		case i == 0:
			return math.Inf(-1), e.Scale, nil
		case i == n+1:
			return e.Scale * math.Pow(e.GrowthFactor, float64(n)), math.Inf(1), nil
		default:
			return e.Scale * math.Pow(e.GrowthFactor, float64(i-1)),
				e.Scale * math.Pow(e.GrowthFactor, float64(i)), nil
		}
	case *pb.Distribution_BucketOptions_LinearBuckets:
		l := opts.LinearBuckets
		n := int(l.NumFiniteBuckets)
		switch {
		case i == 0:
			return math.Inf(-1), l.Offset, nil
		case i == n+1:
			return l.Offset + float64(n)*l.Width, math.Inf(1), nil
		default:
			return l.Offset + float64(i-1)*l.Width, l.Offset + float64(i)*l.Width, nil
		}
	case *pb.Distribution_BucketOptions_ExplicitBuckets:
		bounds := opts.ExplicitBuckets.Bounds
		switch {
		case i == 0:
			return math.Inf(-1), bounds[0], nil
		case i == len(bounds):
			return bounds[len(bounds)-1], math.Inf(1), nil
		default:
			return bounds[i-1], bounds[i], nil
		}
	}
	return 0, 0, fmt.Errorf("unknown bucket options type")
}
