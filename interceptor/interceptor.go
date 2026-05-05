// Package interceptor provides gRPC server interceptors that automatically
// collect request metrics into an embedded SQLite store and expose them via
// a QueryService endpoint for MQL queries.
//
// Usage:
//
//	m, err := interceptor.New(interceptor.Config{ServiceName: "my-service"})
//	if err != nil { ... }
//	defer m.Close()
//
//	srv := grpc.NewServer(
//	    grpc.ChainUnaryInterceptor(m.Unary()),
//	    grpc.ChainStreamInterceptor(m.Stream()),
//	)
//	m.Register(srv) // exposes QueryService on the same server
package interceptor

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	mqlpkg "github.com/accretional/grpc-server-config/internal/metrics/mql"
	"github.com/accretional/grpc-server-config/internal/metrics/query"
	"github.com/accretional/grpc-server-config/internal/metrics/store"
	pb "github.com/accretional/grpc-server-config/pb/metrics"
)

const (
	metricRequestDuration   = "grpc.server/request_duration_ms"
	metricRequestBytes      = "grpc.server/request_bytes"
	metricResponseBytes     = "grpc.server/response_bytes"
	metricDeadlineRemaining = "grpc.server/deadline_remaining_ms"
	metricStreamRecvMsgs    = "grpc.server/stream_recv_messages"
	metricStreamSentMsgs    = "grpc.server/stream_sent_messages"
	metricGoroutines        = "grpc.server/goroutine_count"
	metricHeapBytes         = "grpc.server/heap_bytes"
	metricDroppedPoints     = "grpc.server/dropped_points"
	metricGCLastPauseMS     = "grpc.server/gc_last_pause_ms"
	metricGCPauseTotalMS    = "grpc.server/gc_pause_total_ms"
	resourceType            = "grpc_server"
	resourceTypeHost        = "host"

	writeBufferSize = 4096 // max in-flight metric points before dropping
	writeBatchSize  = 64   // flush to SQLite after this many points
	writeInterval   = time.Second
	purgeInterval   = time.Hour
	sampleInterval  = 30 * time.Second
)

// Config configures the metrics interceptor.
type Config struct {
	// ServiceName is used as the resource name attached to every metric point.
	ServiceName string
	// DBPath is the path to the SQLite database file (default: "metrics.db").
	DBPath string
	// Retention controls how long metric data is kept (default: 7 days).
	Retention time.Duration
}

// Interceptor collects per-request metrics into an embedded SQLite store.
// Metrics are written asynchronously so interceptor overhead on the RPC
// hot path is limited to a non-blocking channel send.
type Interceptor struct {
	store   *store.Store
	service string
	writeCh chan *pb.AnyTimeSeries
	stopCh  chan struct{}
	doneCh  chan struct{}
	dropped int64 // atomic count of points dropped due to full buffer
}

// New creates a new Interceptor. Call Close when the server shuts down.
func New(cfg Config) (*Interceptor, error) {
	if cfg.DBPath == "" {
		cfg.DBPath = "metrics.db"
	}
	if cfg.Retention == 0 {
		cfg.Retention = 7 * 24 * time.Hour
	}

	s, err := store.New(cfg.DBPath, cfg.Retention)
	if err != nil {
		return nil, fmt.Errorf("interceptor: open store: %w", err)
	}

	i := &Interceptor{
		store:   s,
		service: cfg.ServiceName,
		writeCh: make(chan *pb.AnyTimeSeries, writeBufferSize),
		stopCh:  make(chan struct{}),
		doneCh:  make(chan struct{}),
	}
	go i.writeLoop()
	go i.purgeLoop()
	go i.sampleLoop()
	return i, nil
}

// Unary returns a unary server interceptor that records request duration,
// request/response byte sizes, peer address, deadline, and error details.
func (i *Interceptor) Unary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		deadlineRemaining := deadlineRemainingMS(ctx)
		start := time.Now()
		resp, err := handler(ctx, req)
		dur := time.Since(start)

		code := status.Code(err).String()
		errMsg := ""
		if err != nil {
			errMsg = status.Convert(err).Message()
		}

		var reqBytes, respBytes int
		if m, ok := req.(proto.Message); ok {
			reqBytes = proto.Size(m)
		}
		if resp != nil {
			if m, ok := resp.(proto.Message); ok {
				respBytes = proto.Size(m)
			}
		}

		labels := map[string]string{
			"method": info.FullMethod,
			"status": code,
			"error":  errMsg,
			"peer":   peerAddr(ctx),
		}

		i.recordCall(labels, dur, reqBytes, respBytes, deadlineRemaining)
		return resp, err
	}
}

// Stream returns a streaming server interceptor that records duration, message
// counts, peer address, deadline, and error details.
func (i *Interceptor) Stream() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := ss.Context()
		deadlineRemaining := deadlineRemainingMS(ctx)
		cs := &countingStream{ServerStream: ss}

		start := time.Now()
		err := handler(srv, cs)
		dur := time.Since(start)

		code := status.Code(err).String()
		errMsg := ""
		if err != nil {
			errMsg = status.Convert(err).Message()
		}

		labels := map[string]string{
			"method": info.FullMethod,
			"status": code,
			"error":  errMsg,
			"peer":   peerAddr(ctx),
		}

		i.recordCall(labels, dur, 0, 0, deadlineRemaining)
		i.recordStreamCounts(labels, cs)
		return err
	}
}

// Register adds the QueryService to srv, exposing MQL queries over gRPC.
// Call this after attaching the interceptors and before srv.Serve.
func (i *Interceptor) Register(srv *grpc.Server) {
	pb.RegisterQueryServiceServer(srv, query.New(mqlpkg.NewExecutor(i.store)))
}

// Close flushes pending writes, stops background goroutines, and closes the store.
func (i *Interceptor) Close() error {
	close(i.stopCh)
	<-i.doneCh
	return i.store.Close()
}

// ---------------------------------------------------------------------------
// Recording helpers
// ---------------------------------------------------------------------------

func (i *Interceptor) recordCall(labels map[string]string, dur time.Duration, reqBytes, respBytes int, deadlineRemainingMS float64) {
	now := timestamppb.Now()
	res := &pb.Resource{Type: resourceType, Name: i.service}

	i.send(gaugeDouble(metricRequestDuration, labels, now, res, float64(dur.Microseconds())/1000.0))

	if reqBytes > 0 {
		i.send(gaugeInt64(metricRequestBytes, labels, now, res, int64(reqBytes)))
	}
	if respBytes > 0 {
		i.send(gaugeInt64(metricResponseBytes, labels, now, res, int64(respBytes)))
	}
	if deadlineRemainingMS >= 0 {
		i.send(gaugeDouble(metricDeadlineRemaining, labels, now, res, deadlineRemainingMS))
	}
}

func (i *Interceptor) recordStreamCounts(labels map[string]string, cs *countingStream) {
	now := timestamppb.Now()
	res := &pb.Resource{Type: resourceType, Name: i.service}
	recv := atomic.LoadInt64(&cs.recv)
	sent := atomic.LoadInt64(&cs.sent)
	if recv > 0 {
		i.send(gaugeInt64(metricStreamRecvMsgs, labels, now, res, recv))
	}
	if sent > 0 {
		i.send(gaugeInt64(metricStreamSentMsgs, labels, now, res, sent))
	}
}

// ---------------------------------------------------------------------------
// Point constructors
// ---------------------------------------------------------------------------

func gaugeDouble(metricType string, labels map[string]string, at *timestamppb.Timestamp, res *pb.Resource, v float64) *pb.AnyTimeSeries {
	return &pb.AnyTimeSeries{Series: &pb.AnyTimeSeries_Gauge{Gauge: &pb.GaugeTimeSeries{
		Metric:   &pb.Metric{Type: metricType, Labels: labels},
		Resource: res,
		Points:   []*pb.GaugePoint{{At: at, Value: &pb.TypedValue{Value: &pb.TypedValue_DoubleValue{DoubleValue: v}}}},
	}}}
}

func gaugeInt64(metricType string, labels map[string]string, at *timestamppb.Timestamp, res *pb.Resource, v int64) *pb.AnyTimeSeries {
	return &pb.AnyTimeSeries{Series: &pb.AnyTimeSeries_Gauge{Gauge: &pb.GaugeTimeSeries{
		Metric:   &pb.Metric{Type: metricType, Labels: labels},
		Resource: res,
		Points:   []*pb.GaugePoint{{At: at, Value: &pb.TypedValue{Value: &pb.TypedValue_Int64Value{Int64Value: v}}}},
	}}}
}

func (i *Interceptor) send(ts *pb.AnyTimeSeries) {
	select {
	case i.writeCh <- ts:
	default:
		atomic.AddInt64(&i.dropped, 1)
	}
}

// ---------------------------------------------------------------------------
// Context helpers
// ---------------------------------------------------------------------------

// peerAddr returns the client address from the context, or empty string.
func peerAddr(ctx context.Context) string {
	if p, ok := peer.FromContext(ctx); ok {
		return p.Addr.String()
	}
	return ""
}

// deadlineRemainingMS returns ms remaining until the context deadline at call
// start, or -1 if no deadline is set.
func deadlineRemainingMS(ctx context.Context) float64 {
	dl, ok := ctx.Deadline()
	if !ok {
		return -1
	}
	return float64(time.Until(dl).Microseconds()) / 1000.0
}

// ---------------------------------------------------------------------------
// countingStream — wraps grpc.ServerStream to count sent/received messages
// ---------------------------------------------------------------------------

type countingStream struct {
	grpc.ServerStream
	recv int64
	sent int64
}

func (s *countingStream) RecvMsg(m any) error {
	err := s.ServerStream.RecvMsg(m)
	if err == nil {
		atomic.AddInt64(&s.recv, 1)
	}
	return err
}

func (s *countingStream) SendMsg(m any) error {
	err := s.ServerStream.SendMsg(m)
	if err == nil {
		atomic.AddInt64(&s.sent, 1)
	}
	return err
}

// ---------------------------------------------------------------------------
// Background loops
// ---------------------------------------------------------------------------

// writeLoop drains writeCh and batch-writes to SQLite every second or 64 points.
func (i *Interceptor) writeLoop() {
	defer close(i.doneCh)
	ctx := context.Background()
	batch := make([]*pb.AnyTimeSeries, 0, writeBatchSize)
	ticker := time.NewTicker(writeInterval)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		if _, err := i.store.Write(ctx, batch); err != nil {
			log.Printf("interceptor: write metrics: %v", err)
		}
		batch = batch[:0]
	}

	for {
		select {
		case p := <-i.writeCh:
			batch = append(batch, p)
			if len(batch) >= writeBatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-i.stopCh:
			for {
				select {
				case p := <-i.writeCh:
					batch = append(batch, p)
				default:
					flush()
					return
				}
			}
		}
	}
}

// purgeLoop removes metric points older than retention once per hour.
func (i *Interceptor) purgeLoop() {
	ticker := time.NewTicker(purgeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := i.store.Purge(context.Background()); err != nil {
				log.Printf("interceptor: purge: %v", err)
			}
		case <-i.stopCh:
			return
		}
	}
}

// sampleLoop periodically records process-level metrics: goroutine count and heap usage.
func (i *Interceptor) sampleLoop() {
	ticker := time.NewTicker(sampleInterval)
	defer ticker.Stop()
	i.sample() // capture one sample immediately on startup
	for {
		select {
		case <-ticker.C:
			i.sample()
		case <-i.stopCh:
			return
		}
	}
}

func (i *Interceptor) sample() {
	now := timestamppb.Now()
	res := &pb.Resource{Type: resourceType, Name: i.service}
	labels := map[string]string{}

	i.send(gaugeInt64(metricGoroutines, labels, now, res, int64(runtime.NumGoroutine())))
	i.send(gaugeInt64(metricDroppedPoints, labels, now, res, atomic.LoadInt64(&i.dropped)))

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	i.send(gaugeInt64(metricHeapBytes, labels, now, res, int64(ms.HeapInuse)))
	i.send(gaugeDouble(metricGCPauseTotalMS, labels, now, res, float64(ms.PauseTotalNs)/1e6))
	if ms.NumGC > 0 {
		lastPauseNS := ms.PauseNs[(ms.NumGC+255)%256] // ring buffer: last entry
		i.send(gaugeDouble(metricGCLastPauseMS, labels, now, res, float64(lastPauseNS)/1e6))
	}
}
