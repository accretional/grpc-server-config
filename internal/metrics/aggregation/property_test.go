package aggregation

import (
	"math/rand"
	"testing"

	pb "github.com/accretional/grpc-server-config/pb/metrics"
)

func TestProperty_MinLeMeanLeMax(t *testing.T) {
	rng := rand.New(rand.NewSource(99991))
	svc := New()

	for trial := range 50 {
		n := 10 + rng.Intn(100)
		pts := make([]tv, n)
		for i := range pts {
			pts[i] = tv{int64(i + 1), rng.Float64() * 100}
		}
		series := makeGauge(pts...)

		meanPts := gaugeOut(t, mustAlign(t, svc, series, pb.Aligner_ALIGN_MEAN, 60))
		minPts := gaugeOut(t, mustAlign(t, svc, series, pb.Aligner_ALIGN_MIN, 60))
		maxPts := gaugeOut(t, mustAlign(t, svc, series, pb.Aligner_ALIGN_MAX, 60))

		if len(meanPts) != len(minPts) || len(meanPts) != len(maxPts) {
			t.Fatalf("trial %d: length mismatch mean=%d min=%d max=%d",
				trial, len(meanPts), len(minPts), len(maxPts))
		}

		for i := range meanPts {
			if minPts[i].val > meanPts[i].val+1e-10 {
				t.Errorf("trial %d bucket %d: MIN(%v) > MEAN(%v)",
					trial, i, minPts[i].val, meanPts[i].val)
			}
			if meanPts[i].val > maxPts[i].val+1e-10 {
				t.Errorf("trial %d bucket %d: MEAN(%v) > MAX(%v)",
					trial, i, meanPts[i].val, maxPts[i].val)
			}
		}
	}
}

func TestProperty_SumEqualsMeanTimesCount(t *testing.T) {
	rng := rand.New(rand.NewSource(99991))
	svc := New()

	for trial := range 50 {
		n := 10 + rng.Intn(100)
		pts := make([]tv, n)
		for i := range pts {
			pts[i] = tv{int64(i + 1), rng.Float64() * 100}
		}
		series := makeGauge(pts...)

		meanPts := gaugeOut(t, mustAlign(t, svc, series, pb.Aligner_ALIGN_MEAN, 60))
		sumPts := gaugeOut(t, mustAlign(t, svc, series, pb.Aligner_ALIGN_SUM, 60))
		countPts := gaugeOut(t, mustAlign(t, svc, series, pb.Aligner_ALIGN_COUNT, 60))

		if len(meanPts) != len(sumPts) || len(meanPts) != len(countPts) {
			t.Fatalf("trial %d: length mismatch mean=%d sum=%d count=%d",
				trial, len(meanPts), len(sumPts), len(countPts))
		}

		for i := range meanPts {
			expected := meanPts[i].val * countPts[i].val
			if !approxEq(sumPts[i].val, expected) {
				t.Errorf("trial %d bucket %d: SUM(%v) != MEAN(%v)*COUNT(%v) = %v",
					trial, i, sumPts[i].val, meanPts[i].val, countPts[i].val, expected)
			}
		}
	}
}

func TestProperty_StddevNonNegative(t *testing.T) {
	rng := rand.New(rand.NewSource(99991))
	svc := New()

	for trial := range 50 {
		n := 10 + rng.Intn(100)
		pts := make([]tv, n)
		for i := range pts {
			pts[i] = tv{int64(i + 1), rng.Float64() * 100}
		}
		series := makeGauge(pts...)

		stddevPts := gaugeOut(t, mustAlign(t, svc, series, pb.Aligner_ALIGN_STDDEV, 60))
		for i, p := range stddevPts {
			if p.val < 0 {
				t.Errorf("trial %d bucket %d: STDDEV=%v < 0", trial, i, p.val)
			}
		}
	}
}

func TestProperty_AlignMeanIdempotent(t *testing.T) {
	rng := rand.New(rand.NewSource(99991))
	svc := New()

	for trial := range 20 {
		n := 10 + rng.Intn(100)
		pts := make([]tv, n)
		for i := range pts {
			pts[i] = tv{int64(i + 1), rng.Float64() * 100}
		}
		series := makeGauge(pts...)

		aligned1 := mustAlign(t, svc, series, pb.Aligner_ALIGN_MEAN, 60)
		aligned2 := mustAlign(t, svc, aligned1, pb.Aligner_ALIGN_MEAN, 60)

		pts1 := gaugeOut(t, aligned1)
		pts2 := gaugeOut(t, aligned2)

		if len(pts1) != len(pts2) {
			t.Fatalf("trial %d: aligned1 len=%d aligned2 len=%d (not idempotent)",
				trial, len(pts1), len(pts2))
		}

		for i := range pts1 {
			if pts1[i].secs != pts2[i].secs {
				t.Errorf("trial %d bucket %d: ts1=%d ts2=%d (timestamps differ)",
					trial, i, pts1[i].secs, pts2[i].secs)
			}
			if !approxEq(pts1[i].val, pts2[i].val) {
				t.Errorf("trial %d bucket %d: val1=%v val2=%v (not idempotent)",
					trial, i, pts1[i].val, pts2[i].val)
			}
		}
	}
}
