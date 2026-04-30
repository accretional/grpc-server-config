package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"time"

	pb "github.com/accretional/grpc-server-config/pb/metrics"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ---------------------------------------------------------------------------
// GCP REST API types
// ---------------------------------------------------------------------------

type GCPResponse struct {
	TimeSeries    []GCPTimeSeries `json:"timeSeries"`
	NextPageToken string          `json:"nextPageToken"`
}

type GCPTimeSeries struct {
	Metric     GCPMetric         `json:"metric"`
	Resource   GCPResource       `json:"resource"`
	MetricKind string            `json:"metricKind"`
	ValueType  string            `json:"valueType"`
	Points     []GCPPoint        `json:"points"`
}

type GCPMetric struct {
	Type   string            `json:"type"`
	Labels map[string]string `json:"labels"`
}

type GCPResource struct {
	Type   string            `json:"type"`
	Labels map[string]string `json:"labels"`
}

type GCPPoint struct {
	Interval GCPInterval `json:"interval"`
	Value    GCPValue    `json:"value"`
}

type GCPInterval struct {
	StartTime string `json:"startTime"`
	EndTime   string `json:"endTime"`
}

// GCPValue covers the value types we care about.
// Int64 is sent as a string by the JSON API to preserve precision.
type GCPValue struct {
	DoubleValue *float64 `json:"doubleValue,omitempty"`
	Int64Value  *string  `json:"int64Value,omitempty"`
	BoolValue   *bool    `json:"boolValue,omitempty"`
}

// ---------------------------------------------------------------------------
// Fetch
// ---------------------------------------------------------------------------

func gcpToken() (string, error) {
	out, err := exec.Command("gcloud", "auth", "print-access-token").Output()
	if err != nil {
		return "", fmt.Errorf("gcloud auth print-access-token: %w", err)
	}
	return strings.TrimSpace(string(out)), nil
}

// fetchAndSave calls ListTimeSeries (with optional aggregation) and writes
// the full paginated response to path as JSON.
// Pass aligner="" for raw (no aggregation).
func fetchAndSave(project, metricType, aligner string, periodSecs int, start, end time.Time, path string) error {
	token, err := gcpToken()
	if err != nil {
		return err
	}

	var all []GCPTimeSeries
	pageToken := ""
	for {
		resp, next, err := listTimeSeries(token, project, metricType, aligner, periodSecs, start, end, pageToken)
		if err != nil {
			return err
		}
		all = append(all, resp...)
		if next == "" {
			break
		}
		pageToken = next
	}

	// Reverse points within each series: GCP returns newest-first.
	for i := range all {
		slices.Reverse(all[i].Points)
	}

	wrapped := GCPResponse{TimeSeries: all}
	data, err := json.MarshalIndent(wrapped, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

func listTimeSeries(token, project, metricType, aligner string, periodSecs int, start, end time.Time, pageToken string) ([]GCPTimeSeries, string, error) {
	base := fmt.Sprintf("https://monitoring.googleapis.com/v3/projects/%s/timeSeries", project)

	params := url.Values{}
	params.Set("filter", fmt.Sprintf(`metric.type="%s"`, metricType))
	params.Set("interval.startTime", start.Format(time.RFC3339))
	params.Set("interval.endTime", end.Format(time.RFC3339))
	params.Set("pageSize", "1000")
	if aligner != "" {
		params.Set("aggregation.alignmentPeriod", fmt.Sprintf("%ds", periodSecs))
		params.Set("aggregation.perSeriesAligner", aligner)
	}
	if pageToken != "" {
		params.Set("pageToken", pageToken)
	}

	req, _ := http.NewRequest("GET", base+"?"+params.Encode(), nil)
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", err
	}
	if resp.StatusCode != 200 {
		return nil, "", fmt.Errorf("GCP API %d: %s", resp.StatusCode, body)
	}

	var parsed GCPResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, "", err
	}
	return parsed.TimeSeries, parsed.NextPageToken, nil
}

func loadResponse(path string) (*GCPResponse, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var resp GCPResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// ---------------------------------------------------------------------------
// Conversion: GCP → our proto types
// ---------------------------------------------------------------------------

func convertResponse(resp *GCPResponse) ([]*pb.AnyTimeSeries, error) {
	out := make([]*pb.AnyTimeSeries, 0, len(resp.TimeSeries))
	for i := range resp.TimeSeries {
		s, err := convertTimeSeries(&resp.TimeSeries[i])
		if err != nil {
			return nil, fmt.Errorf("series %d (%v): %w", i, resp.TimeSeries[i].Metric.Labels, err)
		}
		out = append(out, s)
	}
	return out, nil
}

func convertTimeSeries(ts *GCPTimeSeries) (*pb.AnyTimeSeries, error) {
	metric := &pb.Metric{Type: ts.Metric.Type, Labels: ts.Metric.Labels}
	resource := &pb.Resource{Name: ts.Resource.Labels["instance_id"], Type: ts.Resource.Type}

	switch ts.MetricKind {
	case "GAUGE":
		return convertGauge(ts, metric, resource)
	case "DELTA":
		return convertDelta(ts, metric, resource)
	default:
		return nil, fmt.Errorf("unsupported MetricKind %q", ts.MetricKind)
	}
}

func convertGauge(ts *GCPTimeSeries, metric *pb.Metric, resource *pb.Resource) (*pb.AnyTimeSeries, error) {
	s := &pb.GaugeTimeSeries{Metric: metric, Resource: resource}
	for _, p := range ts.Points {
		t, err := time.Parse(time.RFC3339, p.Interval.EndTime)
		if err != nil {
			return nil, err
		}
		v, err := gcpValueToTyped(p.Value, ts.ValueType)
		if err != nil {
			return nil, err
		}
		s.Points = append(s.Points, &pb.GaugePoint{
			At:    timestamppb.New(t),
			Value: v,
		})
	}
	return &pb.AnyTimeSeries{Series: &pb.AnyTimeSeries_Gauge{Gauge: s}}, nil
}

func convertDelta(ts *GCPTimeSeries, metric *pb.Metric, resource *pb.Resource) (*pb.AnyTimeSeries, error) {
	if len(ts.Points) == 0 {
		return &pb.AnyTimeSeries{Series: &pb.AnyTimeSeries_Delta{Delta: &pb.DeltaTimeSeries{Metric: metric, Resource: resource}}}, nil
	}

	// GCP DELTA points: [startTime, endTime). GCP raw data has a consistent 1ms
	// offset at window starts (e.g. [T+1ms, T+period]). We snap to whole seconds
	// so durations are exact and windows are contiguous, matching GCP's own arithmetic.
	// Our DeltaTimeSeries: series-level Start + per-point Duration.
	seriesStart, err := parseGCPTime(ts.Points[0].Interval.StartTime)
	if err != nil {
		return nil, err
	}

	s := &pb.DeltaTimeSeries{
		Metric:   metric,
		Resource: resource,
		Start:    timestamppb.New(seriesStart.Truncate(time.Second)),
	}

	for _, p := range ts.Points {
		pStart, err := parseGCPTime(p.Interval.StartTime)
		if err != nil {
			return nil, err
		}
		pEnd, err := parseGCPTime(p.Interval.EndTime)
		if err != nil {
			return nil, err
		}
		v, err := gcpValueToNumeric(p.Value, ts.ValueType)
		if err != nil {
			return nil, err
		}
		// Snap sub-second offsets to whole seconds. GCP raw DELTA data consistently
		// uses [T+1ms, T+period] windows to avoid overlap; the 1ms is an implementation
		// artefact. Snapping to [T, T+period] gives exact contiguity and integer durations
		// that match GCP's own alignment arithmetic (which treats the window as period seconds).
		pStartSnapped := pStart.Truncate(time.Second)
		pEndSnapped := pEnd.Round(time.Second)
		dur := pEndSnapped.Sub(pStartSnapped)
		s.Points = append(s.Points, &pb.NumericPoint{
			Duration: durationpb.New(dur),
			Value:    v,
		})
	}
	return &pb.AnyTimeSeries{Series: &pb.AnyTimeSeries_Delta{Delta: s}}, nil
}

// ---------------------------------------------------------------------------
// Value helpers
// ---------------------------------------------------------------------------

// parseGCPTime parses an RFC3339 timestamp with optional sub-second precision.
func parseGCPTime(s string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		t, err = time.Parse(time.RFC3339, s)
	}
	return t, err
}

func gcpValueToTyped(v GCPValue, valueType string) (*pb.TypedValue, error) {
	switch valueType {
	case "DOUBLE":
		if v.DoubleValue == nil {
			return nil, fmt.Errorf("expected doubleValue")
		}
		return &pb.TypedValue{Value: &pb.TypedValue_DoubleValue{DoubleValue: *v.DoubleValue}}, nil
	case "INT64":
		if v.Int64Value == nil {
			return nil, fmt.Errorf("expected int64Value")
		}
		n, err := strconv.ParseInt(*v.Int64Value, 10, 64)
		if err != nil {
			return nil, err
		}
		return &pb.TypedValue{Value: &pb.TypedValue_Int64Value{Int64Value: n}}, nil
	case "BOOL":
		if v.BoolValue == nil {
			return nil, fmt.Errorf("expected boolValue")
		}
		return &pb.TypedValue{Value: &pb.TypedValue_BoolValue{BoolValue: *v.BoolValue}}, nil
	}
	return nil, fmt.Errorf("unsupported valueType %q", valueType)
}

func gcpValueToNumeric(v GCPValue, valueType string) (*pb.NumericValue, error) {
	switch valueType {
	case "DOUBLE":
		if v.DoubleValue == nil {
			return nil, fmt.Errorf("expected doubleValue")
		}
		return &pb.NumericValue{Value: &pb.NumericValue_DoubleValue{DoubleValue: *v.DoubleValue}}, nil
	case "INT64":
		if v.Int64Value == nil {
			return nil, fmt.Errorf("expected int64Value")
		}
		n, err := strconv.ParseInt(*v.Int64Value, 10, 64)
		if err != nil {
			return nil, err
		}
		return &pb.NumericValue{Value: &pb.NumericValue_Int64Value{Int64Value: n}}, nil
	}
	return nil, fmt.Errorf("unsupported valueType %q for numeric", valueType)
}

