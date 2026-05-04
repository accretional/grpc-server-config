package aggregation

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	pb "github.com/accretional/grpc-server-config/pb/metrics"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// benchGaugeSeries generates n gauge points spaced 1 second apart starting at t=1,
// with random float values in [0,100). Seed is fixed for reproducibility.
func benchGaugeSeries(n int) *pb.AnyTimeSeries {
	rng := rand.New(rand.NewSource(42))
	s := &pb.GaugeTimeSeries{}
	for i := range n {
		s.Points = append(s.Points, &pb.GaugePoint{
			At:    timestamppb.New(time.Unix(int64(i+1), 0)),
			Value: &pb.TypedValue{Value: &pb.TypedValue_DoubleValue{DoubleValue: rng.Float64() * 100}},
		})
	}
	return &pb.AnyTimeSeries{Series: &pb.AnyTimeSeries_Gauge{Gauge: s}}
}

// benchDeltaSeries generates n delta points each with duration=1s and random value.
func benchDeltaSeries(n int) *pb.AnyTimeSeries {
	rng := rand.New(rand.NewSource(42))
	s := &pb.DeltaTimeSeries{Start: timestamppb.New(time.Unix(0, 0))}
	for range n {
		s.Points = append(s.Points, &pb.NumericPoint{
			Duration: durationpb.New(time.Second),
			Value:    &pb.NumericValue{Value: &pb.NumericValue_DoubleValue{DoubleValue: rng.Float64() * 100}},
		})
	}
	return &pb.AnyTimeSeries{Series: &pb.AnyTimeSeries_Delta{Delta: s}}
}

// benchMSeriesN generates m gauge series each with n points at minute boundaries.
func benchMSeriesN(m, n int) []*pb.AnyTimeSeries {
	rng := rand.New(rand.NewSource(42))
	out := make([]*pb.AnyTimeSeries, m)
	for i := range m {
		s := &pb.GaugeTimeSeries{}
		for j := range n {
			s.Points = append(s.Points, &pb.GaugePoint{
				At:    timestamppb.New(time.Unix(int64((j+1)*60), 0)),
				Value: &pb.TypedValue{Value: &pb.TypedValue_DoubleValue{DoubleValue: rng.Float64() * 100}},
			})
		}
		out[i] = &pb.AnyTimeSeries{Series: &pb.AnyTimeSeries_Gauge{Gauge: s}}
	}
	return out
}

func BenchmarkAlignGaugeMean(b *testing.B) {
	svc := New()
	for _, n := range []int{100, 1000, 10000} {
		series := benchGaugeSeries(n)
		req := &pb.AlignRequest{
			Input:           series,
			Aligner:         pb.Aligner_ALIGN_MEAN,
			AlignmentPeriod: durationpb.New(60 * time.Second),
		}
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				svc.Align(context.Background(), req) //nolint:errcheck
			}
		})
	}
}

func BenchmarkAlignDeltaRate(b *testing.B) {
	svc := New()
	for _, n := range []int{100, 1000, 10000} {
		series := benchDeltaSeries(n)
		req := &pb.AlignRequest{
			Input:           series,
			Aligner:         pb.Aligner_ALIGN_RATE,
			AlignmentPeriod: durationpb.New(60 * time.Second),
		}
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				svc.Align(context.Background(), req) //nolint:errcheck
			}
		})
	}
}

func BenchmarkReduceMean(b *testing.B) {
	svc := New()
	for _, tc := range []struct{ m, n int }{{10, 100}, {100, 100}, {10, 10000}} {
		series := benchMSeriesN(tc.m, tc.n)
		req := &pb.ReduceRequest{
			Series:  series,
			Reducer: pb.Reducer_REDUCE_MEAN,
		}
		b.Run(fmt.Sprintf("m=%d_n=%d", tc.m, tc.n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				svc.Reduce(context.Background(), req) //nolint:errcheck
			}
		})
	}
}
