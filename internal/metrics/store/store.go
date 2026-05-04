// Package store provides a SQLite-backed time series store.
// It implements mql.TimeSeriesReader and exposes a Write method for the
// ingest path.
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	_ "modernc.org/sqlite"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/accretional/grpc-server-config/pb/metrics"
)

// series kind constants mirror pb.MetricKind ordinals.
const (
	kindGauge      = 1
	kindDelta      = 2
	kindCumulative = 3
)

// Store is a SQLite-backed time series store.
type Store struct {
	db        *sql.DB
	retention time.Duration
}

// New opens (or creates) the SQLite database at path and initialises the
// schema. retention controls how long individual data points are kept; a
// background call to Purge() removes older points.
func New(path string, retention time.Duration) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("store: open %s: %w", path, err)
	}
	s := &Store{db: db, retention: retention}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

// Close releases the database handle.
func (s *Store) Close() error { return s.db.Close() }

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

const schema = `
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS series (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    resource_type TEXT    NOT NULL,
    metric_type   TEXT    NOT NULL,
    labels_json   TEXT    NOT NULL,
    resource_json TEXT    NOT NULL DEFAULT '{}',
    kind          INTEGER NOT NULL,
    -- Absolute epoch anchor in nanoseconds.
    -- Gauge:      unused (0).
    -- Delta:      series.start.UnixNano().
    -- Cumulative: series.epoch_start.UnixNano().
    anchor_ns     INTEGER NOT NULL DEFAULT 0,
    UNIQUE(resource_type, metric_type, labels_json, kind)
);

CREATE TABLE IF NOT EXISTS points (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    series_id  INTEGER NOT NULL REFERENCES series(id) ON DELETE CASCADE,
    -- Gauge:              point_ns = GaugePoint.at.UnixNano()
    -- Delta/Cumulative:   point_ns = end of window in nanoseconds
    point_ns   INTEGER NOT NULL,
    -- start_ns is 0 for gauge; for delta/cumulative it is the window start.
    start_ns   INTEGER NOT NULL DEFAULT 0,
    -- Exactly one of the value columns is non-NULL.
    bool_val   INTEGER,
    int64_val  INTEGER,
    double_val REAL,
    string_val TEXT,
    dist_json  TEXT
);

CREATE INDEX IF NOT EXISTS idx_points_lookup ON points(series_id, point_ns);
`

func (s *Store) migrate() error {
	_, err := s.db.Exec(schema)
	return err
}

// ---------------------------------------------------------------------------
// Write
// ---------------------------------------------------------------------------

// Write persists a batch of time series. It returns the total number of
// data points written. Series rows are upserted; existing points are never
// modified.
func (s *Store) Write(ctx context.Context, batch []*pb.AnyTimeSeries) (int, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("store: begin tx: %w", err)
	}
	defer tx.Rollback()

	total := 0
	for _, any := range batch {
		n, err := s.writeSeries(ctx, tx, any)
		if err != nil {
			return 0, err
		}
		total += n
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("store: commit: %w", err)
	}
	return total, nil
}

func (s *Store) writeSeries(ctx context.Context, tx *sql.Tx, any *pb.AnyTimeSeries) (int, error) {
	switch ts := any.Series.(type) {
	case *pb.AnyTimeSeries_Gauge:
		return s.writeGauge(ctx, tx, ts.Gauge)
	case *pb.AnyTimeSeries_Delta:
		return s.writeDelta(ctx, tx, ts.Delta)
	case *pb.AnyTimeSeries_Cumulative:
		return s.writeCumulative(ctx, tx, ts.Cumulative)
	default:
		return 0, fmt.Errorf("store: unknown series kind in AnyTimeSeries")
	}
}

func (s *Store) writeGauge(ctx context.Context, tx *sql.Tx, ts *pb.GaugeTimeSeries) (int, error) {
	if ts.Metric == nil {
		return 0, fmt.Errorf("store: GaugeTimeSeries missing metric")
	}
	sid, err := s.upsertSeries(ctx, tx, resourceType(ts.Resource), ts.Metric.Type,
		labelsJSON(ts.Metric.Labels), resourceJSON(ts.Resource), kindGauge, 0)
	if err != nil {
		return 0, err
	}

	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO points(series_id, point_ns, start_ns, bool_val, int64_val, double_val, string_val, dist_json)
		 VALUES (?, ?, 0, ?, ?, ?, ?, ?)`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	for _, p := range ts.Points {
		if p.At == nil || p.Value == nil {
			continue
		}
		b, i, d, str, dj := typedValueCols(p.Value)
		if _, err := stmt.ExecContext(ctx, sid, p.At.AsTime().UnixNano(), b, i, d, str, dj); err != nil {
			return 0, fmt.Errorf("store: insert gauge point: %w", err)
		}
	}
	return len(ts.Points), nil
}

func (s *Store) writeDelta(ctx context.Context, tx *sql.Tx, ts *pb.DeltaTimeSeries) (int, error) {
	if ts.Metric == nil {
		return 0, fmt.Errorf("store: DeltaTimeSeries missing metric")
	}
	anchorNS := int64(0)
	if ts.Start != nil {
		anchorNS = ts.Start.AsTime().UnixNano()
	}
	sid, err := s.upsertSeries(ctx, tx, resourceType(ts.Resource), ts.Metric.Type,
		labelsJSON(ts.Metric.Labels), resourceJSON(ts.Resource), kindDelta, anchorNS)
	if err != nil {
		return 0, err
	}
	return s.writeNumericPoints(ctx, tx, sid, anchorNS, ts.Points)
}

func (s *Store) writeCumulative(ctx context.Context, tx *sql.Tx, ts *pb.CumulativeTimeSeries) (int, error) {
	if ts.Metric == nil {
		return 0, fmt.Errorf("store: CumulativeTimeSeries missing metric")
	}
	anchorNS := int64(0)
	if ts.EpochStart != nil {
		anchorNS = ts.EpochStart.AsTime().UnixNano()
	}
	sid, err := s.upsertSeries(ctx, tx, resourceType(ts.Resource), ts.Metric.Type,
		labelsJSON(ts.Metric.Labels), resourceJSON(ts.Resource), kindCumulative, anchorNS)
	if err != nil {
		return 0, err
	}
	return s.writeNumericPoints(ctx, tx, sid, anchorNS, ts.Points)
}

// writeNumericPoints writes delta/cumulative points, computing absolute start
// and end times from the anchor and cumulative durations.
func (s *Store) writeNumericPoints(ctx context.Context, tx *sql.Tx, sid, anchorNS int64, points []*pb.NumericPoint) (int, error) {
	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO points(series_id, point_ns, start_ns, int64_val, double_val, dist_json)
		 VALUES (?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	runningNS := anchorNS
	for _, p := range points {
		if p.Duration == nil || p.Value == nil {
			continue
		}
		startNS := runningNS
		endNS := runningNS + p.Duration.AsDuration().Nanoseconds()
		runningNS = endNS

		i, d, dj := numericValueCols(p.Value)
		if _, err := stmt.ExecContext(ctx, sid, endNS, startNS, i, d, dj); err != nil {
			return 0, fmt.Errorf("store: insert numeric point: %w", err)
		}
	}
	return len(points), nil
}

// upsertSeries inserts the series row if it doesn't exist and returns its id.
func (s *Store) upsertSeries(ctx context.Context, tx *sql.Tx,
	resType, metricType, labelsJSON, resJSON string, kind, anchorNS int64) (int64, error) {

	_, err := tx.ExecContext(ctx,
		`INSERT OR IGNORE INTO series(resource_type, metric_type, labels_json, resource_json, kind, anchor_ns)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		resType, metricType, labelsJSON, resJSON, kind, anchorNS)
	if err != nil {
		return 0, fmt.Errorf("store: upsert series: %w", err)
	}

	var id int64
	err = tx.QueryRowContext(ctx,
		`SELECT id FROM series WHERE resource_type=? AND metric_type=? AND labels_json=? AND kind=?`,
		resType, metricType, labelsJSON, kind).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("store: lookup series id: %w", err)
	}
	return id, nil
}

// ---------------------------------------------------------------------------
// Read (implements mql.TimeSeriesReader)
// ---------------------------------------------------------------------------

// ReadTimeSeries returns all time series matching resourceType and metricType,
// with points in [start, end), optionally filtered by predicate.
// Predicate filtering is applied in-process after the SQL query.
func (s *Store) ReadTimeSeries(
	ctx context.Context,
	resourceType, metricType string,
	start, end time.Time,
	filter *pb.Predicate,
) ([]*pb.AnyTimeSeries, error) {

	startNS := start.UnixNano()
	endNS := end.UnixNano()

	// Fetch all matching series metadata.
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, labels_json, resource_json, kind, anchor_ns
		 FROM series
		 WHERE resource_type = ? AND metric_type = ?`,
		resourceType, metricType)
	if err != nil {
		return nil, fmt.Errorf("store: query series: %w", err)
	}
	defer rows.Close()

	type seriesMeta struct {
		id           int64
		labelsJSON   string
		resourceJSON string
		kind         int64
		anchorNS     int64
	}
	var metas []seriesMeta
	for rows.Next() {
		var m seriesMeta
		if err := rows.Scan(&m.id, &m.labelsJSON, &m.resourceJSON, &m.kind, &m.anchorNS); err != nil {
			return nil, fmt.Errorf("store: scan series row: %w", err)
		}
		metas = append(metas, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate series: %w", err)
	}

	var out []*pb.AnyTimeSeries
	for _, m := range metas {
		// Parse labels for predicate evaluation.
		var labels map[string]string
		if err := json.Unmarshal([]byte(m.labelsJSON), &labels); err != nil {
			return nil, fmt.Errorf("store: decode labels: %w", err)
		}

		// Apply label filter if provided.
		if filter != nil && !matchPredicate(labels, filter) {
			continue
		}

		// Reconstruct resource from JSON.
		res, err := decodeResource(m.resourceJSON)
		if err != nil {
			return nil, err
		}

		// Fetch points in the time window.
		prows, err := s.db.QueryContext(ctx,
			`SELECT point_ns, start_ns, bool_val, int64_val, double_val, string_val, dist_json
			 FROM points
			 WHERE series_id = ? AND point_ns >= ? AND point_ns < ?
			 ORDER BY point_ns ASC`,
			m.id, startNS, endNS)
		if err != nil {
			return nil, fmt.Errorf("store: query points: %w", err)
		}

		any, err := reconstructSeries(prows, m.kind, m.anchorNS, metricType, labels, res)
		prows.Close()
		if err != nil {
			return nil, err
		}
		if any != nil {
			out = append(out, any)
		}
	}
	return out, nil
}

// reconstructSeries builds an AnyTimeSeries from a set of point rows.
func reconstructSeries(
	rows *sql.Rows,
	kind, anchorNS int64,
	metricType string,
	labels map[string]string,
	res *pb.Resource,
) (*pb.AnyTimeSeries, error) {
	metric := &pb.Metric{Type: metricType, Labels: labels}

	switch kind {
	case kindGauge:
		var points []*pb.GaugePoint
		for rows.Next() {
			var (
				pointNS, startNS int64
				boolVal          sql.NullInt64
				int64Val         sql.NullInt64
				doubleVal        sql.NullFloat64
				stringVal        sql.NullString
				distJSON         sql.NullString
			)
			if err := rows.Scan(&pointNS, &startNS, &boolVal, &int64Val, &doubleVal, &stringVal, &distJSON); err != nil {
				return nil, fmt.Errorf("store: scan gauge point: %w", err)
			}
			tv, err := decodeTypedValue(boolVal, int64Val, doubleVal, stringVal, distJSON)
			if err != nil {
				return nil, err
			}
			points = append(points, &pb.GaugePoint{
				At:    timestamppb.New(time.Unix(0, pointNS)),
				Value: tv,
			})
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		if len(points) == 0 {
			return nil, nil
		}
		return &pb.AnyTimeSeries{Series: &pb.AnyTimeSeries_Gauge{
			Gauge: &pb.GaugeTimeSeries{Metric: metric, Resource: res, Points: points},
		}}, nil

	case kindDelta:
		var points []*pb.NumericPoint
		var seriesStart *timestamppb.Timestamp
		for rows.Next() {
			var (
				pointNS, startNS int64
				boolVal          sql.NullInt64
				int64Val         sql.NullInt64
				doubleVal        sql.NullFloat64
				stringVal        sql.NullString
				distJSON         sql.NullString
			)
			if err := rows.Scan(&pointNS, &startNS, &boolVal, &int64Val, &doubleVal, &stringVal, &distJSON); err != nil {
				return nil, fmt.Errorf("store: scan delta point: %w", err)
			}
			if seriesStart == nil {
				seriesStart = timestamppb.New(time.Unix(0, startNS))
			}
			nv, err := decodeNumericValue(int64Val, doubleVal, distJSON)
			if err != nil {
				return nil, err
			}
			dur := time.Duration(pointNS - startNS)
			points = append(points, &pb.NumericPoint{
				Duration: durationpb.New(dur),
				Value:    nv,
			})
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		if len(points) == 0 {
			return nil, nil
		}
		if seriesStart == nil {
			seriesStart = timestamppb.New(time.Unix(0, anchorNS))
		}
		return &pb.AnyTimeSeries{Series: &pb.AnyTimeSeries_Delta{
			Delta: &pb.DeltaTimeSeries{Metric: metric, Resource: res, Start: seriesStart, Points: points},
		}}, nil

	case kindCumulative:
		var points []*pb.NumericPoint
		epochStart := timestamppb.New(time.Unix(0, anchorNS))
		for rows.Next() {
			var (
				pointNS, startNS int64
				boolVal          sql.NullInt64
				int64Val         sql.NullInt64
				doubleVal        sql.NullFloat64
				stringVal        sql.NullString
				distJSON         sql.NullString
			)
			if err := rows.Scan(&pointNS, &startNS, &boolVal, &int64Val, &doubleVal, &stringVal, &distJSON); err != nil {
				return nil, fmt.Errorf("store: scan cumulative point: %w", err)
			}
			nv, err := decodeNumericValue(int64Val, doubleVal, distJSON)
			if err != nil {
				return nil, err
			}
			dur := time.Duration(pointNS - startNS)
			points = append(points, &pb.NumericPoint{
				Duration: durationpb.New(dur),
				Value:    nv,
			})
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		if len(points) == 0 {
			return nil, nil
		}
		return &pb.AnyTimeSeries{Series: &pb.AnyTimeSeries_Cumulative{
			Cumulative: &pb.CumulativeTimeSeries{Metric: metric, Resource: res, EpochStart: epochStart, Points: points},
		}}, nil

	default:
		return nil, fmt.Errorf("store: unknown series kind %d", kind)
	}
}

// ---------------------------------------------------------------------------
// Purge
// ---------------------------------------------------------------------------

// Purge deletes points older than the store's retention window.
func (s *Store) Purge(ctx context.Context) error {
	cutoffNS := time.Now().Add(-s.retention).UnixNano()
	_, err := s.db.ExecContext(ctx, `DELETE FROM points WHERE point_ns < ?`, cutoffNS)
	return err
}

// ---------------------------------------------------------------------------
// Predicate evaluation
// ---------------------------------------------------------------------------

// matchPredicate evaluates a filter predicate against a label map.
// label_paths like "metric.labels.zone" are resolved by stripping the prefix.
func matchPredicate(labels map[string]string, pred *pb.Predicate) bool {
	if pred == nil {
		return true
	}
	switch expr := pred.Expr.(type) {
	case *pb.Predicate_Comparison:
		return matchComparison(labels, expr.Comparison)
	case *pb.Predicate_Logical:
		return matchLogical(labels, expr.Logical)
	case *pb.Predicate_Not:
		return !matchPredicate(labels, expr.Not.Operand)
	}
	return true
}

func matchComparison(labels map[string]string, c *pb.ComparisonExpr) bool {
	key := resolveLabelPath(c.LabelPath)
	actual, ok := labels[key]
	if !ok {
		return false
	}
	rhs := valueString(c.Rhs)

	switch c.Op {
	case pb.ComparisonOp_EQ:
		return actual == rhs
	case pb.ComparisonOp_NEQ:
		return actual != rhs
	case pb.ComparisonOp_RE:
		matched, _ := regexp.MatchString(rhs, actual)
		return matched
	case pb.ComparisonOp_NRE:
		matched, _ := regexp.MatchString(rhs, actual)
		return !matched
	case pb.ComparisonOp_LT:
		return actual < rhs
	case pb.ComparisonOp_LTE:
		return actual <= rhs
	case pb.ComparisonOp_GT:
		return actual > rhs
	case pb.ComparisonOp_GTE:
		return actual >= rhs
	}
	return false
}

func matchLogical(labels map[string]string, l *pb.LogicalExpr) bool {
	switch l.Op {
	case pb.LogicalOp_AND:
		for _, op := range l.Operands {
			if !matchPredicate(labels, op) {
				return false
			}
		}
		return true
	case pb.LogicalOp_OR:
		for _, op := range l.Operands {
			if matchPredicate(labels, op) {
				return true
			}
		}
		return false
	}
	return true
}

// resolveLabelPath strips known prefixes to get the raw label key.
func resolveLabelPath(path string) string {
	for _, prefix := range []string{
		"metric.labels.",
		"resource.labels.",
		"metadata.system_labels.",
		"metadata.user_labels.",
	} {
		if strings.HasPrefix(path, prefix) {
			return path[len(prefix):]
		}
	}
	return path
}

func valueString(v *pb.Value) string {
	if v == nil {
		return ""
	}
	switch val := v.V.(type) {
	case *pb.Value_StringValue:
		return val.StringValue
	case *pb.Value_NumberValue:
		return fmt.Sprintf("%g", val.NumberValue)
	case *pb.Value_BoolValue:
		if val.BoolValue {
			return "true"
		}
		return "false"
	}
	return ""
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// labelsJSON produces a stable JSON representation of a label map for use as
// a unique key. Keys are sorted lexicographically.
func labelsJSON(m map[string]string) string {
	if len(m) == 0 {
		return "{}"
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	b, _ := json.Marshal(func() map[string]string {
		out := make(map[string]string, len(m))
		for _, k := range keys {
			out[k] = m[k]
		}
		return out
	}())
	return string(b)
}

func resourceType(r *pb.Resource) string {
	if r == nil {
		return ""
	}
	return r.Type
}

func resourceJSON(r *pb.Resource) string {
	if r == nil {
		return "{}"
	}
	b, err := protojson.Marshal(r)
	if err != nil {
		return "{}"
	}
	return string(b)
}

func decodeResource(j string) (*pb.Resource, error) {
	if j == "" || j == "{}" {
		return nil, nil
	}
	r := &pb.Resource{}
	if err := protojson.Unmarshal([]byte(j), r); err != nil {
		return nil, fmt.Errorf("store: decode resource: %w", err)
	}
	return r, nil
}

// typedValueCols decomposes a TypedValue into the five SQLite columns.
func typedValueCols(tv *pb.TypedValue) (boolVal, int64Val *int64, doubleVal *float64, stringVal *string, distJSON *string) {
	if tv == nil {
		return
	}
	switch v := tv.Value.(type) {
	case *pb.TypedValue_BoolValue:
		var i int64
		if v.BoolValue {
			i = 1
		}
		boolVal = &i
	case *pb.TypedValue_Int64Value:
		int64Val = &v.Int64Value
	case *pb.TypedValue_DoubleValue:
		doubleVal = &v.DoubleValue
	case *pb.TypedValue_StringValue:
		stringVal = &v.StringValue
	case *pb.TypedValue_DistributionValue:
		b, _ := protojson.Marshal(v.DistributionValue)
		s := string(b)
		distJSON = &s
	}
	return
}

// numericValueCols decomposes a NumericValue into the three relevant columns.
func numericValueCols(nv *pb.NumericValue) (int64Val *int64, doubleVal *float64, distJSON *string) {
	if nv == nil {
		return
	}
	switch v := nv.Value.(type) {
	case *pb.NumericValue_Int64Value:
		int64Val = &v.Int64Value
	case *pb.NumericValue_DoubleValue:
		doubleVal = &v.DoubleValue
	case *pb.NumericValue_DistributionValue:
		b, _ := protojson.Marshal(v.DistributionValue)
		s := string(b)
		distJSON = &s
	}
	return
}

func decodeTypedValue(boolVal, int64Val sql.NullInt64, doubleVal sql.NullFloat64, stringVal, distJSON sql.NullString) (*pb.TypedValue, error) {
	switch {
	case boolVal.Valid:
		return &pb.TypedValue{Value: &pb.TypedValue_BoolValue{BoolValue: boolVal.Int64 != 0}}, nil
	case int64Val.Valid:
		return &pb.TypedValue{Value: &pb.TypedValue_Int64Value{Int64Value: int64Val.Int64}}, nil
	case doubleVal.Valid:
		return &pb.TypedValue{Value: &pb.TypedValue_DoubleValue{DoubleValue: doubleVal.Float64}}, nil
	case stringVal.Valid:
		return &pb.TypedValue{Value: &pb.TypedValue_StringValue{StringValue: stringVal.String}}, nil
	case distJSON.Valid:
		d := &pb.Distribution{}
		if err := protojson.Unmarshal([]byte(distJSON.String), d); err != nil {
			return nil, fmt.Errorf("store: decode distribution: %w", err)
		}
		return &pb.TypedValue{Value: &pb.TypedValue_DistributionValue{DistributionValue: d}}, nil
	}
	return &pb.TypedValue{}, nil
}

func decodeNumericValue(int64Val sql.NullInt64, doubleVal sql.NullFloat64, distJSON sql.NullString) (*pb.NumericValue, error) {
	switch {
	case int64Val.Valid:
		return &pb.NumericValue{Value: &pb.NumericValue_Int64Value{Int64Value: int64Val.Int64}}, nil
	case doubleVal.Valid:
		return &pb.NumericValue{Value: &pb.NumericValue_DoubleValue{DoubleValue: doubleVal.Float64}}, nil
	case distJSON.Valid:
		d := &pb.Distribution{}
		if err := protojson.Unmarshal([]byte(distJSON.String), d); err != nil {
			return nil, fmt.Errorf("store: decode distribution: %w", err)
		}
		return &pb.NumericValue{Value: &pb.NumericValue_DistributionValue{DistributionValue: d}}, nil
	}
	return &pb.NumericValue{}, nil
}
