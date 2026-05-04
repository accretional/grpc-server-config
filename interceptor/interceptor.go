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
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	mqlpkg "github.com/accretional/grpc-server-config/internal/metrics/mql"
	"github.com/accretional/grpc-server-config/internal/metrics/query"
	"github.com/accretional/grpc-server-config/internal/metrics/store"
	pb "github.com/accretional/grpc-server-config/pb/metrics"
)

const (
	metricRequestDuration = "grpc.server/request_duration_ms"
	resourceType          = "grpc_server"

	writeBufferSize = 4096 // max in-flight metric points before dropping
	writeBatchSize  = 64   // flush to SQLite after this many points
	writeInterval   = time.Second
	purgeInterval   = time.Hour
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
	go i.purgeLoop(cfg.Retention)
	return i, nil
}

// Unary returns a unary server interceptor that records request duration.
func (i *Interceptor) Unary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		i.record(info.FullMethod, status.Code(err).String(), time.Since(start))
		return resp, err
	}
}

// Stream returns a streaming server interceptor that records request duration.
func (i *Interceptor) Stream() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()
		err := handler(srv, ss)
		i.record(info.FullMethod, status.Code(err).String(), time.Since(start))
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

// record enqueues a single latency point. Drops silently if the buffer is full.
func (i *Interceptor) record(method, code string, dur time.Duration) {
	point := &pb.AnyTimeSeries{
		Series: &pb.AnyTimeSeries_Gauge{
			Gauge: &pb.GaugeTimeSeries{
				Metric: &pb.Metric{
					Type:   metricRequestDuration,
					Labels: map[string]string{"method": method, "status": code},
				},
				Resource: &pb.Resource{Type: resourceType, Name: i.service},
				Points: []*pb.GaugePoint{{
					At: timestamppb.Now(),
					Value: &pb.TypedValue{Value: &pb.TypedValue_DoubleValue{
						DoubleValue: float64(dur.Microseconds()) / 1000.0,
					}},
				}},
			},
		},
	}
	select {
	case i.writeCh <- point:
	default:
		// buffer full — metrics are best-effort, drop rather than block the RPC
	}
}

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
			// drain remaining points before exiting
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
func (i *Interceptor) purgeLoop(retention time.Duration) {
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
