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

type gcpResponse struct {
	TimeSeries    []gcpTimeSeries `json:"timeSeries"`
	NextPageToken string          `json:"nextPageToken"`
}

type gcpTimeSeries struct {
	Metric     gcpMetric         `json:"metric"`
	Resource   gcpResource       `json:"resource"`
	MetricKind string            `json:"metricKind"`
	ValueType  string            `json:"valueType"`
	Points     []gcpPoint        `json:"points"`
}

type gcpMetric struct {
	Type   string            `json:"type"`
	Labels map[string]string `json:"labels"`
}

type gcpResource struct {
	Type   string            `json:"type"`
	Labels map[string]string `json:"labels"`
}

type gcpPoint struct {
	Interval gcpInterval `json:"interval"`
	Value    gcpValue    `json:"value"`
}

type gcpInterval struct {
	StartTime string `json:"startTime"`
	EndTime   string `json:"endTime"`
}

// gcpValue covers the value types we care about.
// Int64 is sent as a string by the JSON API to preserve precision.
type gcpValue struct {
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
func fetchAndSave(project, metricType, aligner, crossSeriesReducer string, groupByFields []string, periodSecs int, start, end time.Time, path string) error {
	token, err := gcpToken()
	if err != nil {
		return err
	}

	var all []gcpTimeSeries
	pageToken := ""
	for {
		resp, next, err := listTimeSeries(token, project, metricType, aligner, crossSeriesReducer, groupByFields, periodSecs, start, end, pageToken)
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

	wrapped := gcpResponse{TimeSeries: all}
	data, err := json.MarshalIndent(wrapped, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

func listTimeSeries(token, project, metricType, aligner, crossSeriesReducer string, groupByFields []string, periodSecs int, start, end time.Time, pageToken string) ([]gcpTimeSeries, string, error) {
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
	if crossSeriesReducer != "" {
		params.Set("aggregation.crossSeriesReducer", crossSeriesReducer)
		for _, f := range groupByFields {
			if !strings.HasPrefix(f, "metric.") && !strings.HasPrefix(f, "resource.") {
				f = "metric.labels." + f
			}
			params.Add("aggregation.groupByFields", f)
		}
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

	var parsed gcpResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, "", err
	}
	return parsed.TimeSeries, parsed.NextPageToken, nil
}

func loadResponse(path string) (*gcpResponse, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var resp gcpResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// ---------------------------------------------------------------------------
// Conversion: GCP → our proto types
// ---------------------------------------------------------------------------

func convertTimeSeries(ts *gcpTimeSeries) (*pb.AnyTimeSeries, error) {
	metric := &pb.Metric{Type: ts.Metric.Type, Labels: ts.Metric.Labels}
	resource := &pb.Resource{Name: ts.Resource.Labels["instance_id"], Type: ts.Resource.Type}

	switch ts.MetricKind {
	case "GAUGE":
		return convertGauge(ts, metric, resource)
	case "DELTA":
		return convertDelta(ts, metric, resource)
	case "CUMULATIVE":
		return convertCumulative(ts, metric, resource)
	default:
		return nil, fmt.Errorf("unsupported MetricKind %q", ts.MetricKind)
	}
}

func convertGauge(ts *gcpTimeSeries, metric *pb.Metric, resource *pb.Resource) (*pb.AnyTimeSeries, error) {
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

func convertDelta(ts *gcpTimeSeries, metric *pb.Metric, resource *pb.Resource) (*pb.AnyTimeSeries, error) {
	if len(ts.Points) == 0 {
		return &pb.AnyTimeSeries{Series: &pb.AnyTimeSeries_Delta{Delta: &pb.DeltaTimeSeries{Metric: metric, Resource: resource}}}, nil
	}

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
		dur := pEnd.Round(time.Second).Sub(pStart.Truncate(time.Second))
		s.Points = append(s.Points, &pb.NumericPoint{
			Duration: durationpb.New(dur),
			Value:    v,
		})
	}
	return &pb.AnyTimeSeries{Series: &pb.AnyTimeSeries_Delta{Delta: s}}, nil
}

func convertCumulative(ts *gcpTimeSeries, metric *pb.Metric, resource *pb.Resource) (*pb.AnyTimeSeries, error) {
	if len(ts.Points) == 0 {
		return &pb.AnyTimeSeries{Series: &pb.AnyTimeSeries_Cumulative{
			Cumulative: &pb.CumulativeTimeSeries{Metric: metric, Resource: resource},
		}}, nil
	}
	epochStart, err := parseGCPTime(ts.Points[0].Interval.StartTime)
	if err != nil {
		return nil, err
	}
	epochStartSnapped := epochStart.Truncate(time.Second)
	s := &pb.CumulativeTimeSeries{
		Metric:     metric,
		Resource:   resource,
		EpochStart: timestamppb.New(epochStartSnapped),
	}
	prevEnd := epochStartSnapped
	for _, p := range ts.Points {
		pEnd, err := parseGCPTime(p.Interval.EndTime)
		if err != nil {
			return nil, err
		}
		pEndSnapped := pEnd.Round(time.Second)
		v, err := gcpValueToNumeric(p.Value, ts.ValueType)
		if err != nil {
			return nil, err
		}
		s.Points = append(s.Points, &pb.NumericPoint{
			Duration: durationpb.New(pEndSnapped.Sub(prevEnd)),
			Value:    v,
		})
		prevEnd = pEndSnapped
	}
	return &pb.AnyTimeSeries{Series: &pb.AnyTimeSeries_Cumulative{Cumulative: s}}, nil
}

// ---------------------------------------------------------------------------
// Value helpers
// ---------------------------------------------------------------------------

func parseGCPTime(s string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		t, err = time.Parse(time.RFC3339, s)
	}
	return t, err
}

func gcpValueToTyped(v gcpValue, valueType string) (*pb.TypedValue, error) {
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

func gcpValueToNumeric(v gcpValue, valueType string) (*pb.NumericValue, error) {
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
