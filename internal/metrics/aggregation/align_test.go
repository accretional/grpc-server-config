package aggregation

import (
	"context"
	"math"
	"testing"
	"time"

	pb "github.com/accretional/grpc-server-config/pb/metrics"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestAlignGauge_StatAligner(t *testing.T) {
	svc := New()

	type statCase struct {
		aligner  pb.Aligner
		expected []tv
	}

	stddevOf123 := math.Sqrt(2.0 / 3.0) // population stddev of [1,2,3]

	tests := []struct {
		name   string
		input  *pb.AnyTimeSeries
		period int64
		cases  []statCase
	}{
		{
			name:   "oneBucket",
			input:  makeGauge(tv{10, 1}, tv{30, 2}, tv{50, 3}),
			period: 60,
			cases: []statCase{
				{pb.Aligner_ALIGN_MEAN, []tv{{60, 2.0}}},
				{pb.Aligner_ALIGN_MIN, []tv{{60, 1.0}}},
				{pb.Aligner_ALIGN_MAX, []tv{{60, 3.0}}},
				{pb.Aligner_ALIGN_SUM, []tv{{60, 6.0}}},
				{pb.Aligner_ALIGN_COUNT, []tv{{60, 3.0}}},
				{pb.Aligner_ALIGN_STDDEV, []tv{{60, stddevOf123}}},
			},
		},
		{
			name:   "twoBuckets",
			input:  makeGauge(tv{10, 1}, tv{50, 3}, tv{70, 5}, tv{110, 7}),
			period: 60,
			cases: []statCase{
				{pb.Aligner_ALIGN_MEAN, []tv{{60, 2.0}, {120, 6.0}}},
				{pb.Aligner_ALIGN_MIN, []tv{{60, 1.0}, {120, 5.0}}},
				{pb.Aligner_ALIGN_MAX, []tv{{60, 3.0}, {120, 7.0}}},
				{pb.Aligner_ALIGN_SUM, []tv{{60, 4.0}, {120, 12.0}}},
				{pb.Aligner_ALIGN_COUNT, []tv{{60, 2.0}, {120, 2.0}}},
				{pb.Aligner_ALIGN_STDDEV, []tv{{60, 1.0}, {120, 1.0}}},
			},
		},
		{
			name:   "singlePoint",
			input:  makeGauge(tv{30, 5.0}),
			period: 60,
			cases: []statCase{
				{pb.Aligner_ALIGN_MEAN, []tv{{60, 5.0}}},
				{pb.Aligner_ALIGN_MIN, []tv{{60, 5.0}}},
				{pb.Aligner_ALIGN_MAX, []tv{{60, 5.0}}},
				{pb.Aligner_ALIGN_SUM, []tv{{60, 5.0}}},
				{pb.Aligner_ALIGN_COUNT, []tv{{60, 1.0}}},
				{pb.Aligner_ALIGN_STDDEV, []tv{{60, 0.0}}},
			},
		},
		{
			name:   "empty",
			input:  makeGauge(),
			period: 60,
			cases: []statCase{
				{pb.Aligner_ALIGN_MEAN, nil},
				{pb.Aligner_ALIGN_MIN, nil},
				{pb.Aligner_ALIGN_MAX, nil},
				{pb.Aligner_ALIGN_SUM, nil},
				{pb.Aligner_ALIGN_COUNT, nil},
				{pb.Aligner_ALIGN_STDDEV, nil},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, sc := range tt.cases {
				t.Run(sc.aligner.String(), func(t *testing.T) {
					out := mustAlign(t, svc, tt.input, sc.aligner, tt.period)
					pts := gaugeOut(t, out)
					if len(pts) != len(sc.expected) {
						t.Fatalf("len=%d want %d", len(pts), len(sc.expected))
					}
					for i, want := range sc.expected {
						if pts[i].secs != want.secs {
							t.Errorf("pt[%d].secs=%d want %d", i, pts[i].secs, want.secs)
						}
						if !approxEq(pts[i].val, want.val) {
							t.Errorf("pt[%d].val=%v want %v", i, pts[i].val, want.val)
						}
					}
				})
			}
		})
	}
}

func TestAlignGauge_BoundaryPoint(t *testing.T) {
	svc := New()

	t.Run("point_at_exactly_60_belongs_to_bucket0", func(t *testing.T) {
		// bucketIdx(60, 60) = ceil(60/60)-1 = 0 → output at t=60
		out := mustAlign(t, svc, makeGauge(tv{60, 5.0}), pb.Aligner_ALIGN_MEAN, 60)
		pts := gaugeOut(t, out)
		if len(pts) != 1 {
			t.Fatalf("len=%d want 1", len(pts))
		}
		if pts[0].secs != 60 {
			t.Errorf("secs=%d want 60", pts[0].secs)
		}
		if !approxEq(pts[0].val, 5.0) {
			t.Errorf("val=%v want 5.0", pts[0].val)
		}
	})

	t.Run("point_at_61_belongs_to_bucket1", func(t *testing.T) {
		// bucketIdx(61, 60) = ceil(61/60)-1 = 1 → output at t=120
		out := mustAlign(t, svc, makeGauge(tv{61, 7.0}), pb.Aligner_ALIGN_MEAN, 60)
		pts := gaugeOut(t, out)
		if len(pts) != 1 {
			t.Fatalf("len=%d want 1", len(pts))
		}
		if pts[0].secs != 120 {
			t.Errorf("secs=%d want 120", pts[0].secs)
		}
		if !approxEq(pts[0].val, 7.0) {
			t.Errorf("val=%v want 7.0", pts[0].val)
		}
	})
}

func TestAlignGauge_Interpolate(t *testing.T) {
	svc := New()

	t.Run("two_points_with_gap", func(t *testing.T) {
		// Input: tv{60,0}, tv{180,60}, period=60
		// firstIdx=bucketIdx(60,60)=0, lastIdx=bucketIdx(180,60)+1=3
		// Expected 4 points:
		//   idx=0, t=60:  exact match → 0.0
		//   idx=1, t=120: interpolate [60→0, 180→60] at 120 → 30.0
		//   idx=2, t=180: exact match → 60.0
		//   idx=3, t=240: extrapolate → slope=(60-0)/(180-60)=0.5/s → 60+0.5*60=90.0
		out := mustAlign(t, svc, makeGauge(tv{60, 0}, tv{180, 60}), pb.Aligner_ALIGN_INTERPOLATE, 60)
		pts := gaugeOut(t, out)
		expected := []tv{{60, 0.0}, {120, 30.0}, {180, 60.0}, {240, 90.0}}
		if len(pts) != len(expected) {
			t.Fatalf("len=%d want %d", len(pts), len(expected))
		}
		for i, want := range expected {
			if pts[i].secs != want.secs {
				t.Errorf("pt[%d].secs=%d want %d", i, pts[i].secs, want.secs)
			}
			if !approxEq(pts[i].val, want.val) {
				t.Errorf("pt[%d].val=%v want %v", i, pts[i].val, want.val)
			}
		}
	})

	t.Run("single_point_passthrough", func(t *testing.T) {
		out := mustAlign(t, svc, makeGauge(tv{60, 5}), pb.Aligner_ALIGN_INTERPOLATE, 60)
		pts := gaugeOut(t, out)
		expected := []tv{{60, 5}}
		if len(pts) != len(expected) {
			t.Fatalf("len=%d want %d", len(pts), len(expected))
		}
		if pts[0].secs != 60 || !approxEq(pts[0].val, 5) {
			t.Errorf("pt[0]=%v want {60 5}", pts[0])
		}
	})

	t.Run("empty_passthrough", func(t *testing.T) {
		out := mustAlign(t, svc, makeGauge(), pb.Aligner_ALIGN_INTERPOLATE, 60)
		pts := gaugeOut(t, out)
		if len(pts) != 0 {
			t.Errorf("len=%d want 0", len(pts))
		}
	})
}

func TestAlignGauge_NextOlder(t *testing.T) {
	svc := New()

	t.Run("carry_forward_with_gap", func(t *testing.T) {
		// Input: tv{60,1}, tv{180,3}, period=60
		// firstIdx=0, lastIdx=bucketIdx(180,60)+1=3
		//   idx=0, bucketEnd=60:  pt[0].At=60 ≤ 60 → lastVal=1.0 → (60,1)
		//   idx=1, bucketEnd=120: pt[1].At=180 > 120 → carry 1.0 → (120,1)
		//   idx=2, bucketEnd=180: pt[1].At=180 ≤ 180 → lastVal=3.0 → (180,3)
		//   idx=3, bucketEnd=240: no more pts → carry 3.0 → (240,3)
		out := mustAlign(t, svc, makeGauge(tv{60, 1}, tv{180, 3}), pb.Aligner_ALIGN_NEXT_OLDER, 60)
		pts := gaugeOut(t, out)
		expected := []tv{{60, 1}, {120, 1}, {180, 3}, {240, 3}}
		if len(pts) != len(expected) {
			t.Fatalf("len=%d want %d", len(pts), len(expected))
		}
		for i, want := range expected {
			if pts[i].secs != want.secs {
				t.Errorf("pt[%d].secs=%d want %d", i, pts[i].secs, want.secs)
			}
			if !approxEq(pts[i].val, want.val) {
				t.Errorf("pt[%d].val=%v want %v", i, pts[i].val, want.val)
			}
		}
	})

	t.Run("single_point_carries_one_extra_bucket", func(t *testing.T) {
		// Input: tv{60,5}, lastIdx=0+1=1
		//   idx=0 → (60,5), idx=1 → (120,5)
		out := mustAlign(t, svc, makeGauge(tv{60, 5}), pb.Aligner_ALIGN_NEXT_OLDER, 60)
		pts := gaugeOut(t, out)
		expected := []tv{{60, 5}, {120, 5}}
		if len(pts) != len(expected) {
			t.Fatalf("len=%d want %d", len(pts), len(expected))
		}
		for i, want := range expected {
			if pts[i].secs != want.secs {
				t.Errorf("pt[%d].secs=%d want %d", i, pts[i].secs, want.secs)
			}
			if !approxEq(pts[i].val, want.val) {
				t.Errorf("pt[%d].val=%v want %v", i, pts[i].val, want.val)
			}
		}
	})

	t.Run("empty_passthrough", func(t *testing.T) {
		out := mustAlign(t, svc, makeGauge(), pb.Aligner_ALIGN_NEXT_OLDER, 60)
		pts := gaugeOut(t, out)
		if len(pts) != 0 {
			t.Errorf("len=%d want 0", len(pts))
		}
	})
}

func TestAlignGauge_PercentChange(t *testing.T) {
	svc := New()
	// All sub-tests use smoothingWindow = period = 60s so that:
	//   current  window = (T-60s, T]   ≡ bucket ending at T
	//   previous window = (T-120s, T-60s] ≡ bucket ending at T-60s
	// This makes expected values easy to reason about without needing
	// data spanning the default 10-minute window.
	const period, ws = int64(60), int64(60)

	t.Run("ten_percent_increase", func(t *testing.T) {
		// previous window (0,60]: t=10 (100), t=50 (100) → mean=100
		// current  window (60,120]: t=70 (110), t=110 (110) → mean=110
		// PCT = ((110-100)/100)*100 = 10.0
		out := mustAlignWithSmoothingWindow(t, svc, makeGauge(tv{10, 100}, tv{50, 100}, tv{70, 110}, tv{110, 110}), pb.Aligner_ALIGN_PERCENT_CHANGE, period, ws)
		pts := gaugeOut(t, out)
		if len(pts) != 1 {
			t.Fatalf("len=%d want 1", len(pts))
		}
		if pts[0].secs != 120 || !approxEq(pts[0].val, 10.0) {
			t.Errorf("pt[0]=%v want {120 10.0}", pts[0])
		}
	})

	t.Run("ten_percent_decrease", func(t *testing.T) {
		// previous window (0,60]: t=10 (100), t=50 (100) → mean=100
		// current  window (60,120]: t=70 (90), t=110 (90) → mean=90
		// PCT = ((90-100)/100)*100 = -10.0
		out := mustAlignWithSmoothingWindow(t, svc, makeGauge(tv{10, 100}, tv{50, 100}, tv{70, 90}, tv{110, 90}), pb.Aligner_ALIGN_PERCENT_CHANGE, period, ws)
		pts := gaugeOut(t, out)
		if len(pts) != 1 {
			t.Fatalf("len=%d want 1", len(pts))
		}
		if pts[0].secs != 120 || !approxEq(pts[0].val, -10.0) {
			t.Errorf("pt[0]=%v want {120 -10.0}", pts[0])
		}
	})

	t.Run("prev_zero_curr_nonzero_returns_inf", func(t *testing.T) {
		// previous window (0,60]: t=10 (0) → mean=0
		// current  window (60,120]: t=70 (5) → mean=5
		// prev==0, curr!=0 → +Inf per Cloud Monitoring spec
		out := mustAlignWithSmoothingWindow(t, svc, makeGauge(tv{10, 0}, tv{70, 5}), pb.Aligner_ALIGN_PERCENT_CHANGE, period, ws)
		pts := gaugeOut(t, out)
		if len(pts) != 1 {
			t.Fatalf("len=%d want 1", len(pts))
		}
		if pts[0].secs != 120 || !math.IsInf(pts[0].val, 1) {
			t.Errorf("pt[0]=%v want {120 +Inf}", pts[0])
		}
	})

	t.Run("both_zero_returns_zero", func(t *testing.T) {
		// previous window (0,60]: t=10 (0) → mean=0
		// current  window (60,120]: t=70 (0) → mean=0
		// both==0 → 0 per Cloud Monitoring spec
		out := mustAlignWithSmoothingWindow(t, svc, makeGauge(tv{10, 0}, tv{70, 0}), pb.Aligner_ALIGN_PERCENT_CHANGE, period, ws)
		pts := gaugeOut(t, out)
		if len(pts) != 1 {
			t.Fatalf("len=%d want 1", len(pts))
		}
		if pts[0].secs != 120 || !approxEq(pts[0].val, 0.0) {
			t.Errorf("pt[0]=%v want {120 0.0}", pts[0])
		}
	})

	t.Run("negative_values_excluded", func(t *testing.T) {
		// Negative values are treated as missing per spec.
		// previous window (0,60]: t=10 (-50, excluded), t=50 (100) → mean=100
		// current  window (60,120]: t=70 (110), t=90 (-20, excluded) → mean=110
		// PCT = ((110-100)/100)*100 = 10.0
		out := mustAlignWithSmoothingWindow(t, svc, makeGauge(tv{10, -50}, tv{50, 100}, tv{70, 110}, tv{90, -20}), pb.Aligner_ALIGN_PERCENT_CHANGE, period, ws)
		pts := gaugeOut(t, out)
		if len(pts) != 1 {
			t.Fatalf("len=%d want 1", len(pts))
		}
		if pts[0].secs != 120 || !approxEq(pts[0].val, 10.0) {
			t.Errorf("pt[0]=%v want {120 10.0}", pts[0])
		}
	})

	t.Run("single_bucket_no_output", func(t *testing.T) {
		// Only one bucket of data → previous window is empty → no output point.
		out := mustAlignWithSmoothingWindow(t, svc, makeGauge(tv{10, 5}), pb.Aligner_ALIGN_PERCENT_CHANGE, period, ws)
		pts := gaugeOut(t, out)
		if len(pts) != 0 {
			t.Errorf("len=%d want 0", len(pts))
		}
	})

	t.Run("default_smoothing_window_used_when_unset", func(t *testing.T) {
		// When PercentChangeSmoothingWindow is not set, the default (10 min) is used.
		// With data spanning only 2 minutes, both windows overlap fully → non-zero output.
		out := mustAlign(t, svc, makeGauge(tv{10, 100}, tv{70, 110}), pb.Aligner_ALIGN_PERCENT_CHANGE, period)
		pts := gaugeOut(t, out)
		// With a 10-min window at t=120:
		//   current  (-480,120]: both points → mean=105
		//   previous (-540,60]:  t=10 only   → mean=100
		// PCT = ((105-100)/100)*100 = 5.0
		if len(pts) != 1 {
			t.Fatalf("len=%d want 1", len(pts))
		}
		if pts[0].secs != 120 || !approxEq(pts[0].val, 5.0) {
			t.Errorf("pt[0]=%v want {120 5.0}", pts[0])
		}
	})
}

func TestAlignDelta_Rate(t *testing.T) {
	svc := New()

	t.Run("single_window_2_per_second", func(t *testing.T) {
		// makeDelta(0, dp{60, 120.0}): window [0,60), val=120
		// bucketIdx(0,60)=-1 (start is at exactly a boundary), bucketIdx(59.999..,60)=0
		// b=-1: zero-overlap contribution → rate=0 at t=0
		// b=0: full overlap → rate=120/60=2.0 at t=60
		out := mustAlign(t, svc, makeDelta(0, dp{60, 120.0}), pb.Aligner_ALIGN_RATE, 60)
		pts := gaugeOut(t, out)
		// Find the non-zero rate point at t=60
		found := false
		for _, p := range pts {
			if p.secs == 60 {
				if !approxEq(p.val, 2.0) {
					t.Errorf("pt at t=60: val=%v want 2.0", p.val)
				}
				found = true
			}
		}
		if !found {
			t.Errorf("no point found at t=60 in output %v", pts)
		}
	})

	t.Run("two_windows", func(t *testing.T) {
		// Window[0,60)→rate=2.0 at t=60, Window[60,120)→rate=3.0 at t=120
		out := mustAlign(t, svc, makeDelta(0, dp{60, 120}, dp{60, 180}), pb.Aligner_ALIGN_RATE, 60)
		pts := gaugeOut(t, out)
		// Check that t=60 has rate≈2.0 and t=120 has rate≈3.0
		want := map[int64]float64{60: 2.0, 120: 3.0}
		for _, p := range pts {
			if wantVal, ok := want[p.secs]; ok {
				if !approxEq(p.val, wantVal) {
					t.Errorf("pt at t=%d: val=%v want %v", p.secs, p.val, wantVal)
				}
				delete(want, p.secs)
			}
		}
		for ts := range want {
			t.Errorf("missing expected point at t=%d", ts)
		}
	})

	t.Run("empty_passthrough", func(t *testing.T) {
		// Empty delta → early return, output delta with 0 points
		out := mustAlign(t, svc, makeDelta(0), pb.Aligner_ALIGN_RATE, 60)
		_, vals, _ := deltaOut(t, out)
		if len(vals) != 0 {
			t.Errorf("len=%d want 0", len(vals))
		}
	})
}

func TestAlignDelta_Rebucket(t *testing.T) {
	svc := New()

	t.Run("two_30s_windows_into_one_60s", func(t *testing.T) {
		// dp{30,50} + dp{30,70}: both windows within bucket [0,60)
		// Window 1 [0,30): startB=bucketIdx(0,60)=-1 (boundary), but only bucket 0 gets real overlap
		// Bucket 0 receives 50+70=120
		// Note: bucket -1 may also appear with value 0 due to zero-overlap map entry
		out := mustAlign(t, svc, makeDelta(0, dp{30, 50}, dp{30, 70}), pb.Aligner_ALIGN_DELTA, 60)
		_, vals, durs := deltaOut(t, out)
		// Find the bucket with value≈120
		found := false
		for i, v := range vals {
			if approxEq(v, 120.0) {
				if durs[i] != 60 {
					t.Errorf("bucket with val=120: dur=%d want 60", durs[i])
				}
				found = true
			} else if !approxEq(v, 0.0) {
				t.Errorf("unexpected non-zero, non-120 value: %v", v)
			}
		}
		if !found {
			t.Errorf("no bucket with value≈120 found in %v", vals)
		}
	})

	t.Run("already_aligned", func(t *testing.T) {
		out := mustAlign(t, svc, makeDelta(0, dp{60, 100}), pb.Aligner_ALIGN_DELTA, 60)
		_, vals, durs := deltaOut(t, out)
		// Find the bucket with value≈100
		found := false
		for i, v := range vals {
			if approxEq(v, 100.0) {
				if durs[i] != 60 {
					t.Errorf("bucket with val=100: dur=%d want 60", durs[i])
				}
				found = true
			} else if !approxEq(v, 0.0) {
				t.Errorf("unexpected non-zero, non-100 value: %v", v)
			}
		}
		if !found {
			t.Errorf("no bucket with value≈100 found in %v", vals)
		}
	})
}

func TestAlignDelta_Stat(t *testing.T) {
	svc := New()

	t.Run("mean_assigns_by_midpoint", func(t *testing.T) {
		// makeDelta(0, dp{60,60}, dp{60,120}):
		// Window 1: [0,60) midpoint=30s → bucket 0; value=60
		// Window 2: [60,120) midpoint=90s → bucket 1; value=120
		// ALIGN_MEAN of single-point buckets = the point itself
		out := mustAlign(t, svc, makeDelta(0, dp{60, 60}, dp{60, 120}), pb.Aligner_ALIGN_MEAN, 60)
		pts := gaugeOut(t, out)
		expected := []tv{{60, 60}, {120, 120}}
		if len(pts) != len(expected) {
			t.Fatalf("len=%d want %d", len(pts), len(expected))
		}
		for i, want := range expected {
			if pts[i].secs != want.secs {
				t.Errorf("pt[%d].secs=%d want %d", i, pts[i].secs, want.secs)
			}
			if !approxEq(pts[i].val, want.val) {
				t.Errorf("pt[%d].val=%v want %v", i, pts[i].val, want.val)
			}
		}
	})
}

func TestAlignCumulative_Rate(t *testing.T) {
	svc := New()

	// makeCumulative(0, dp{60,60}, dp{60,120}, dp{60,180}): constant rate 1.0/s
	// epochStart=0 → firstBucket=bucketIdx(0,60)=-1 → pre-epoch bucket at t=0 with rate=0
	// In-range buckets at t=60, t=120 must have rate=1.0
	out := mustAlign(t, svc, makeCumulative(0, dp{60, 60}, dp{60, 120}, dp{60, 180}), pb.Aligner_ALIGN_RATE, 60)
	pts := gaugeOut(t, out)

	if len(pts) < 3 {
		t.Fatalf("len=%d want >=3", len(pts))
	}

	// First point: pre-epoch bucket, rate=0
	if pts[0].secs != 0 {
		t.Errorf("pts[0].secs=%d want 0 (pre-epoch)", pts[0].secs)
	}
	if !approxEq(pts[0].val, 0.0) {
		t.Errorf("pts[0].val=%v want 0.0 (pre-epoch rate)", pts[0].val)
	}

	// All subsequent points: rate=1.0
	for i := 1; i < len(pts); i++ {
		if !approxEq(pts[i].val, 1.0) {
			t.Errorf("pts[%d].val=%v want 1.0", i, pts[i].val)
		}
	}
}

func TestAlignCumulative_Delta(t *testing.T) {
	svc := New()

	// Same input: epochStart=0
	// out.Start = time.Unix(firstBucket*ps, 0) = time.Unix(-60, 0)
	// Pre-epoch bucket: delta=0.0
	// In-range buckets: delta=60.0 each
	out := mustAlign(t, svc, makeCumulative(0, dp{60, 60}, dp{60, 120}, dp{60, 180}), pb.Aligner_ALIGN_DELTA, 60)
	startSecs, vals, durs := deltaOut(t, out)

	if startSecs != -60 {
		t.Errorf("startSecs=%d want -60", startSecs)
	}

	if len(vals) < 2 {
		t.Fatalf("len(vals)=%d want >=2", len(vals))
	}

	// First value is pre-epoch delta=0
	if !approxEq(vals[0], 0.0) {
		t.Errorf("vals[0]=%v want 0.0 (pre-epoch delta)", vals[0])
	}

	// All subsequent deltas = 60.0
	for i := 1; i < len(vals); i++ {
		if !approxEq(vals[i], 60.0) {
			t.Errorf("vals[%d]=%v want 60.0", i, vals[i])
		}
	}

	// All durations = 60
	for i, d := range durs {
		if d != 60 {
			t.Errorf("durs[%d]=%d want 60", i, d)
		}
	}
}

func TestAlignCumulative_CounterReset_Delta(t *testing.T) {
	svc := New()

	// epochStart=0, times=[60,120,180,240], vals=[100,200,50,150]
	// Reset detected at index 2: vals[2]=50 < vals[1]=200
	// Seg0: {epoch=0, times=[60,120], vals=[100,200]}
	// Seg1: {epoch=120, times=[180,240], vals=[50,150]}
	//
	// firstBucket = bucketIdx(0, 60) = -1 → startSecs = -60
	// lastBucket  = bucketIdx(240, 60) = 3
	// Bucket -1 [−60,  0): delta = 0    (pre-epoch, no contributions)
	// Bucket  0 [  0, 60): delta = 100  (seg0: interp(60)-interp(0) = 100-0)
	// Bucket  1 [ 60,120): delta = 100  (seg0: interp(120)-interp(60) = 200-100)
	// Bucket  2 [120,180): delta = 50   (seg1: interp(180)-interp(120) = 50-0)
	out := mustAlign(t, svc,
		makeCumulative(0, dp{60, 100}, dp{60, 200}, dp{60, 50}, dp{60, 150}),
		pb.Aligner_ALIGN_DELTA, 60)
	startSecs, vals, durs := deltaOut(t, out)

	if startSecs != -60 {
		t.Errorf("startSecs=%d want -60", startSecs)
	}
	if len(vals) != 4 {
		t.Fatalf("len(vals)=%d want 4", len(vals))
	}
	expectedVals := []float64{0, 100, 100, 50}
	for i, want := range expectedVals {
		if !approxEq(vals[i], want) {
			t.Errorf("vals[%d]=%v want %v (counter reset: bucket should not go negative)", i, vals[i], want)
		}
	}
	for i, d := range durs {
		if d != 60 {
			t.Errorf("durs[%d]=%d want 60", i, d)
		}
	}
}

func TestAlignCumulative_CounterReset_Rate(t *testing.T) {
	svc := New()

	// Same input as CounterReset_Delta above. Rate = delta / period.
	// t=0:   rate = 0/60   = 0.0
	// t=60:  rate = 100/60 ≈ 1.6667
	// t=120: rate = 100/60 ≈ 1.6667
	// t=180: rate = 50/60  ≈ 0.8333
	out := mustAlign(t, svc,
		makeCumulative(0, dp{60, 100}, dp{60, 200}, dp{60, 50}, dp{60, 150}),
		pb.Aligner_ALIGN_RATE, 60)
	pts := gaugeOut(t, out)

	if len(pts) != 4 {
		t.Fatalf("len(pts)=%d want 4", len(pts))
	}
	wantPts := []tv{
		{0, 0.0},
		{60, 100.0 / 60},
		{120, 100.0 / 60},
		{180, 50.0 / 60},
	}
	for i, want := range wantPts {
		if pts[i].secs != want.secs {
			t.Errorf("pts[%d].secs=%d want %d", i, pts[i].secs, want.secs)
		}
		if !approxEq(pts[i].val, want.val) {
			t.Errorf("pts[%d].val=%v want %v (counter reset: rate must not go negative)", i, pts[i].val, want.val)
		}
	}
}

func TestAlign_Errors(t *testing.T) {
	svc := New()
	ctx := context.Background()

	t.Run("nil_input", func(t *testing.T) {
		_, err := svc.Align(ctx, &pb.AlignRequest{
			Input:           nil,
			Aligner:         pb.Aligner_ALIGN_MEAN,
			AlignmentPeriod: durationpb.New(60 * time.Second),
		})
		if err == nil {
			t.Error("expected error for nil input, got nil")
		}
	})

	t.Run("zero_period", func(t *testing.T) {
		_, err := svc.Align(ctx, &pb.AlignRequest{
			Input:           makeGauge(tv{60, 1}),
			Aligner:         pb.Aligner_ALIGN_MEAN,
			AlignmentPeriod: durationpb.New(0),
		})
		if err == nil {
			t.Error("expected error for zero period, got nil")
		}
	})

	t.Run("unsupported_aligner_for_cumulative", func(t *testing.T) {
		_, err := svc.Align(ctx, &pb.AlignRequest{
			Input:           makeCumulative(0, dp{60, 60}),
			Aligner:         pb.Aligner_ALIGN_MEAN,
			AlignmentPeriod: durationpb.New(60 * time.Second),
		})
		if err == nil {
			t.Error("expected error for ALIGN_MEAN on cumulative series, got nil")
		}
	})

	t.Run("unknown_series_type", func(t *testing.T) {
		_, err := svc.Align(ctx, &pb.AlignRequest{
			Input:           &pb.AnyTimeSeries{},
			Aligner:         pb.Aligner_ALIGN_MEAN,
			AlignmentPeriod: durationpb.New(60 * time.Second),
		})
		if err == nil {
			t.Error("expected error for AnyTimeSeries with nil Series, got nil")
		}
	})

	t.Run("percentile_on_non_distribution_gauge", func(t *testing.T) {
		_, err := svc.Align(ctx, &pb.AlignRequest{
			Input:           makeGauge(tv{30, 5.0}),
			Aligner:         pb.Aligner_ALIGN_PERCENTILE_50,
			AlignmentPeriod: durationpb.New(60 * time.Second),
		})
		if err == nil {
			t.Error("expected error for double value with ALIGN_PERCENTILE_50, got nil")
		}
	})

	t.Run("merge_distributions_nil_bucket_options", func(t *testing.T) {
		// First point has no BucketOptions; second triggers mergeDistributions → error.
		d1 := &pb.Distribution{Count: 1, BucketCounts: []int64{0, 1, 0}}
		d2 := &pb.Distribution{
			Count: 1,
			BucketOptions: &pb.Distribution_BucketOptions{
				Options: &pb.Distribution_BucketOptions_ExplicitBuckets{
					ExplicitBuckets: &pb.Distribution_BucketOptions_Explicit{Bounds: []float64{0, 10}},
				},
			},
			BucketCounts: []int64{0, 1, 0},
		}
		_, err := svc.Align(ctx, &pb.AlignRequest{
			Input:           makeGaugeDist(distPt{10, d1}, distPt{30, d2}),
			Aligner:         pb.Aligner_ALIGN_PERCENTILE_50,
			AlignmentPeriod: durationpb.New(60 * time.Second),
		})
		if err == nil {
			t.Error("expected error for nil BucketOptions in mergeDistributions, got nil")
		}
	})

	t.Run("merge_distributions_different_bucket_counts", func(t *testing.T) {
		opts3 := &pb.Distribution_BucketOptions{
			Options: &pb.Distribution_BucketOptions_ExplicitBuckets{
				ExplicitBuckets: &pb.Distribution_BucketOptions_Explicit{Bounds: []float64{0, 10}},
			},
		}
		opts4 := &pb.Distribution_BucketOptions{
			Options: &pb.Distribution_BucketOptions_ExplicitBuckets{
				ExplicitBuckets: &pb.Distribution_BucketOptions_Explicit{Bounds: []float64{0, 5, 10}},
			},
		}
		d1 := &pb.Distribution{Count: 1, BucketOptions: opts3, BucketCounts: []int64{0, 1, 0}}
		d2 := &pb.Distribution{Count: 1, BucketOptions: opts4, BucketCounts: []int64{0, 0, 1, 0}}
		_, err := svc.Align(ctx, &pb.AlignRequest{
			Input:           makeGaugeDist(distPt{10, d1}, distPt{30, d2}),
			Aligner:         pb.Aligner_ALIGN_PERCENTILE_50,
			AlignmentPeriod: durationpb.New(60 * time.Second),
		})
		if err == nil {
			t.Error("expected error for different bucket counts in mergeDistributions, got nil")
		}
	})
}

func TestAlignGauge_Bool(t *testing.T) {
	svc := New()

	// Three points all in bucket [0,60): two true, one false.
	oneBucket := makeGaugeBool(bv{10, true}, bv{30, false}, bv{50, true})

	t.Run("COUNT_TRUE_one_bucket", func(t *testing.T) {
		out := mustAlign(t, svc, oneBucket, pb.Aligner_ALIGN_COUNT_TRUE, 60)
		pts := gaugeOut(t, out)
		if len(pts) != 1 {
			t.Fatalf("len=%d want 1", len(pts))
		}
		if pts[0].secs != 60 || !approxEq(pts[0].val, 2.0) {
			t.Errorf("pt[0]=%v want {60 2}", pts[0])
		}
	})

	t.Run("COUNT_FALSE_one_bucket", func(t *testing.T) {
		out := mustAlign(t, svc, oneBucket, pb.Aligner_ALIGN_COUNT_FALSE, 60)
		pts := gaugeOut(t, out)
		if len(pts) != 1 {
			t.Fatalf("len=%d want 1", len(pts))
		}
		if pts[0].secs != 60 || !approxEq(pts[0].val, 1.0) {
			t.Errorf("pt[0]=%v want {60 1}", pts[0])
		}
	})

	t.Run("FRACTION_TRUE_one_bucket", func(t *testing.T) {
		out := mustAlign(t, svc, oneBucket, pb.Aligner_ALIGN_FRACTION_TRUE, 60)
		pts := gaugeOut(t, out)
		if len(pts) != 1 {
			t.Fatalf("len=%d want 1", len(pts))
		}
		if pts[0].secs != 60 || !approxEq(pts[0].val, 2.0/3.0) {
			t.Errorf("pt[0]=%v want {60 %.6f}", pts[0], 2.0/3.0)
		}
	})

	t.Run("COUNT_TRUE_two_buckets", func(t *testing.T) {
		// Bucket 0 [0,60): t=10(T), t=50(F) → trueN=1
		// Bucket 1 [60,120): t=70(F), t=90(T), t=110(T) → trueN=2
		in2 := makeGaugeBool(bv{10, true}, bv{50, false}, bv{70, false}, bv{90, true}, bv{110, true})
		out := mustAlign(t, svc, in2, pb.Aligner_ALIGN_COUNT_TRUE, 60)
		pts := gaugeOut(t, out)
		expected := []tv{{60, 1.0}, {120, 2.0}}
		if len(pts) != len(expected) {
			t.Fatalf("len=%d want %d", len(pts), len(expected))
		}
		for i, want := range expected {
			if pts[i].secs != want.secs || !approxEq(pts[i].val, want.val) {
				t.Errorf("pt[%d]=%v want %v", i, pts[i], want)
			}
		}
	})

	t.Run("FRACTION_TRUE_two_buckets", func(t *testing.T) {
		// Bucket 0 [0,60): t=10(T), t=50(F) → 0.5
		// Bucket 1 [60,120): t=70(F), t=90(T), t=110(T) → 2/3
		in2 := makeGaugeBool(bv{10, true}, bv{50, false}, bv{70, false}, bv{90, true}, bv{110, true})
		out := mustAlign(t, svc, in2, pb.Aligner_ALIGN_FRACTION_TRUE, 60)
		pts := gaugeOut(t, out)
		expected := []tv{{60, 0.5}, {120, 2.0 / 3.0}}
		if len(pts) != len(expected) {
			t.Fatalf("len=%d want %d", len(pts), len(expected))
		}
		for i, want := range expected {
			if pts[i].secs != want.secs || !approxEq(pts[i].val, want.val) {
				t.Errorf("pt[%d]=%v want %v", i, pts[i], want)
			}
		}
	})

	t.Run("FRACTION_TRUE_all_false", func(t *testing.T) {
		out := mustAlign(t, svc, makeGaugeBool(bv{10, false}, bv{30, false}), pb.Aligner_ALIGN_FRACTION_TRUE, 60)
		pts := gaugeOut(t, out)
		if len(pts) != 1 {
			t.Fatalf("len=%d want 1", len(pts))
		}
		if !approxEq(pts[0].val, 0.0) {
			t.Errorf("val=%v want 0.0", pts[0].val)
		}
	})
}

func TestAlignGauge_Percentile(t *testing.T) {
	svc := New()

	// Shared linear bucket schema: N=3, width=10, offset=0.
	// Bucket indices: 0→(-∞,0), 1→(0,10), 2→(10,20), 3→(20,30), 4→(30,+∞)
	linearOpts := &pb.Distribution_BucketOptions{
		Options: &pb.Distribution_BucketOptions_LinearBuckets{
			LinearBuckets: &pb.Distribution_BucketOptions_Linear{
				NumFiniteBuckets: 3,
				Width:            10,
				Offset:           0,
			},
		},
	}

	// All 4 counts fall in bucket 1 (0,10).
	// PERCENTILE_p: target=p*4, frac=target/4 → val = p*10.
	allInBucket1 := &pb.Distribution{
		Count:         4,
		Mean:          5,
		BucketOptions: linearOpts,
		BucketCounts:  []int64{0, 4, 0, 0, 0},
	}

	t.Run("PERCENTILE_50_linear", func(t *testing.T) {
		// target=2, frac=2/4=0.5 → 0+0.5*10=5.0
		out := mustAlign(t, svc, makeGaugeDist(distPt{30, allInBucket1}), pb.Aligner_ALIGN_PERCENTILE_50, 60)
		pts := gaugeOut(t, out)
		if len(pts) != 1 {
			t.Fatalf("len=%d want 1", len(pts))
		}
		if pts[0].secs != 60 || !approxEq(pts[0].val, 5.0) {
			t.Errorf("pt[0]=%v want {60 5.0}", pts[0])
		}
	})

	t.Run("PERCENTILE_99_linear", func(t *testing.T) {
		// target=3.96, frac=3.96/4=0.99 → 9.9
		out := mustAlign(t, svc, makeGaugeDist(distPt{30, allInBucket1}), pb.Aligner_ALIGN_PERCENTILE_99, 60)
		pts := gaugeOut(t, out)
		if len(pts) != 1 {
			t.Fatalf("len=%d want 1", len(pts))
		}
		if pts[0].secs != 60 || !approxEq(pts[0].val, 9.9) {
			t.Errorf("pt[0]=%v want {60 9.9}", pts[0])
		}
	})

	t.Run("PERCENTILE_95_linear", func(t *testing.T) {
		// target=3.8, frac=3.8/4=0.95 → 9.5
		out := mustAlign(t, svc, makeGaugeDist(distPt{30, allInBucket1}), pb.Aligner_ALIGN_PERCENTILE_95, 60)
		pts := gaugeOut(t, out)
		if len(pts) != 1 {
			t.Fatalf("len=%d want 1", len(pts))
		}
		if pts[0].secs != 60 || !approxEq(pts[0].val, 9.5) {
			t.Errorf("pt[0]=%v want {60 9.5}", pts[0])
		}
	})

	t.Run("PERCENTILE_05_linear", func(t *testing.T) {
		// target=0.2, frac=0.2/4=0.05 → 0.5
		out := mustAlign(t, svc, makeGaugeDist(distPt{30, allInBucket1}), pb.Aligner_ALIGN_PERCENTILE_05, 60)
		pts := gaugeOut(t, out)
		if len(pts) != 1 {
			t.Fatalf("len=%d want 1", len(pts))
		}
		if pts[0].secs != 60 || !approxEq(pts[0].val, 0.5) {
			t.Errorf("pt[0]=%v want {60 0.5}", pts[0])
		}
	})

	t.Run("PERCENTILE_50_exponential", func(t *testing.T) {
		// Exponential: scale=1, growthFactor=2, N=2 → buckets (-∞,1),(1,2),(2,4),(4,+∞)
		// All 4 counts in bucket 1 (1,2).
		// target=2; frac=2/4=0.5 → 1+0.5*(2-1)=1.5
		expDist := &pb.Distribution{
			Count: 4,
			Mean:  1.5,
			BucketOptions: &pb.Distribution_BucketOptions{
				Options: &pb.Distribution_BucketOptions_ExponentialBuckets{
					ExponentialBuckets: &pb.Distribution_BucketOptions_Exponential{
						NumFiniteBuckets: 2,
						GrowthFactor:     2,
						Scale:            1,
					},
				},
			},
			BucketCounts: []int64{0, 4, 0, 0},
		}
		out := mustAlign(t, svc, makeGaugeDist(distPt{30, expDist}), pb.Aligner_ALIGN_PERCENTILE_50, 60)
		pts := gaugeOut(t, out)
		if len(pts) != 1 {
			t.Fatalf("len=%d want 1", len(pts))
		}
		if pts[0].secs != 60 || !approxEq(pts[0].val, 1.5) {
			t.Errorf("pt[0]=%v want {60 1.5}", pts[0])
		}
	})

	t.Run("PERCENTILE_50_explicit", func(t *testing.T) {
		// Explicit: bounds=[0,10] → buckets (-∞,0),(0,10),(10,+∞)
		// All 4 counts in bucket 1 (0,10).
		// target=2; frac=2/4=0.5 → 0+0.5*10=5.0
		explDist := &pb.Distribution{
			Count: 4,
			Mean:  5,
			BucketOptions: &pb.Distribution_BucketOptions{
				Options: &pb.Distribution_BucketOptions_ExplicitBuckets{
					ExplicitBuckets: &pb.Distribution_BucketOptions_Explicit{
						Bounds: []float64{0, 10},
					},
				},
			},
			BucketCounts: []int64{0, 4, 0},
		}
		out := mustAlign(t, svc, makeGaugeDist(distPt{30, explDist}), pb.Aligner_ALIGN_PERCENTILE_50, 60)
		pts := gaugeOut(t, out)
		if len(pts) != 1 {
			t.Fatalf("len=%d want 1", len(pts))
		}
		if pts[0].secs != 60 || !approxEq(pts[0].val, 5.0) {
			t.Errorf("pt[0]=%v want {60 5.0}", pts[0])
		}
	})

	t.Run("PERCENTILE_50_overflow_clamped_by_range", func(t *testing.T) {
		// All 4 counts in overflow bucket (30,+∞); Range=[32,48].
		// target=2; upper clamped to 48; frac=2/4=0.5 → 30+0.5*(48-30)=39.0
		overflowDist := &pb.Distribution{
			Count:         4,
			Mean:          40,
			BucketOptions: linearOpts,
			BucketCounts:  []int64{0, 0, 0, 0, 4},
			Range:         &pb.Distribution_Range{Min: 32, Max: 48},
		}
		out := mustAlign(t, svc, makeGaugeDist(distPt{30, overflowDist}), pb.Aligner_ALIGN_PERCENTILE_50, 60)
		pts := gaugeOut(t, out)
		if len(pts) != 1 {
			t.Fatalf("len=%d want 1", len(pts))
		}
		if pts[0].secs != 60 || !approxEq(pts[0].val, 39.0) {
			t.Errorf("pt[0]=%v want {60 39.0}", pts[0])
		}
	})

	t.Run("PERCENTILE_50_underflow_clamped_by_range", func(t *testing.T) {
		// All 4 counts in underflow bucket (-∞,0); Range=[-8,-1].
		// lower clamped to -8, upper=0; frac=2/4=0.5 → -8+0.5*(0-(-8))=-4.0
		underflowDist := &pb.Distribution{
			Count:         4,
			Mean:          -4,
			BucketOptions: linearOpts,
			BucketCounts:  []int64{4, 0, 0, 0, 0},
			Range:         &pb.Distribution_Range{Min: -8, Max: -1},
		}
		out := mustAlign(t, svc, makeGaugeDist(distPt{30, underflowDist}), pb.Aligner_ALIGN_PERCENTILE_50, 60)
		pts := gaugeOut(t, out)
		if len(pts) != 1 {
			t.Fatalf("len=%d want 1", len(pts))
		}
		if pts[0].secs != 60 || !approxEq(pts[0].val, -4.0) {
			t.Errorf("pt[0]=%v want {60 -4.0}", pts[0])
		}
	})

	t.Run("PERCENTILE_50_two_points_merged", func(t *testing.T) {
		// Two distribution points in the same 60s bucket (t=10 and t=30):
		//   d1: Count=2, BucketCounts=[0,2,0,0,0] (bucket 1: 0-10)
		//   d2: Count=2, BucketCounts=[0,0,2,0,0] (bucket 2: 10-20)
		// Merged: Count=4, BucketCounts=[0,2,2,0,0]
		// target=2; cumulative at i=1=2≥2 → bucket 1 (0,10)
		// frac=(2-0)/2=1.0 → 0+1.0*10=10.0
		d1 := &pb.Distribution{Count: 2, BucketOptions: linearOpts, BucketCounts: []int64{0, 2, 0, 0, 0}}
		d2 := &pb.Distribution{Count: 2, BucketOptions: linearOpts, BucketCounts: []int64{0, 0, 2, 0, 0}}
		out := mustAlign(t, svc, makeGaugeDist(distPt{10, d1}, distPt{30, d2}), pb.Aligner_ALIGN_PERCENTILE_50, 60)
		pts := gaugeOut(t, out)
		if len(pts) != 1 {
			t.Fatalf("len=%d want 1", len(pts))
		}
		if pts[0].secs != 60 || !approxEq(pts[0].val, 10.0) {
			t.Errorf("pt[0]=%v want {60 10.0}", pts[0])
		}
	})
}

func TestAlignDelta_Stat_AllAligners(t *testing.T) {
	svc := New()

	// Two 30s windows both with midpoint inside bucket [0,60):
	//   Window 1: [0,30)  midpoint=15 → bucketIdx(15,60)=0, value=10
	//   Window 2: [30,60) midpoint=45 → bucketIdx(45,60)=0, value=20
	// applyStat sees vals=[10,20] → mean=15, stddev=sqrt(25)=5
	input := makeDelta(0, dp{30, 10}, dp{30, 20})

	cases := []struct {
		aligner  pb.Aligner
		expected float64
	}{
		{pb.Aligner_ALIGN_MIN, 10.0},
		{pb.Aligner_ALIGN_MAX, 20.0},
		{pb.Aligner_ALIGN_SUM, 30.0},
		{pb.Aligner_ALIGN_COUNT, 2.0},
		{pb.Aligner_ALIGN_STDDEV, 5.0}, // sqrt(((10-15)²+(20-15)²)/2)
	}

	for _, tc := range cases {
		t.Run(tc.aligner.String(), func(t *testing.T) {
			out := mustAlign(t, svc, input, tc.aligner, 60)
			pts := gaugeOut(t, out)
			if len(pts) != 1 {
				t.Fatalf("len=%d want 1", len(pts))
			}
			if pts[0].secs != 60 {
				t.Errorf("secs=%d want 60", pts[0].secs)
			}
			if !approxEq(pts[0].val, tc.expected) {
				t.Errorf("aligner %v: val=%v want %v", tc.aligner, pts[0].val, tc.expected)
			}
		})
	}
}
