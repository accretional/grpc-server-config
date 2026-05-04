// cmd/metric-server runs the unified metrics gRPC server.
//
// It hosts three services on a single port:
//   - MetricService   — ingest endpoint for agents (WriteTimeSeries)
//   - QueryService    — MQL query endpoint for dashboards and CLIs (Query)
//   - AggregationService — align/reduce building block (Align, Reduce)
//
// Usage:
//
//	go run ./cmd/metric-server [flags]
//
// Flags:
//
//	-addr      TCP address to listen on (default ":50051", overridden by $GRPC_PORT)
//	-db        Path to the SQLite database file (default "metrics.db")
//	-retention Retention window for time series data (default "168h" = 7 days)
//	-tls       Enable TLS (requires -cert-file and -key-file)
//	-cert-file Path to TLS certificate file
//	-key-file  Path to TLS private key file
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/reflection"

	"github.com/accretional/grpc-server-config/internal/metrics/aggregation"
	"github.com/accretional/grpc-server-config/internal/metrics/metric"
	mqlpkg "github.com/accretional/grpc-server-config/internal/metrics/mql"
	"github.com/accretional/grpc-server-config/internal/metrics/query"
	"github.com/accretional/grpc-server-config/internal/metrics/store"
	pb "github.com/accretional/grpc-server-config/pb/metrics"
)

func main() {
	addr := flag.String("addr", ":50051", "TCP address to listen on (overridden by $GRPC_PORT)")
	dbPath := flag.String("db", "metrics.db", "SQLite database file path")
	retentionStr := flag.String("retention", "168h", "Data retention window (e.g. 24h, 7d)")
	tlsEnabled := flag.Bool("tls", false, "Enable TLS")
	certFile := flag.String("cert-file", "", "TLS certificate file (required when -tls is set)")
	keyFile := flag.String("key-file", "", "TLS private key file (required when -tls is set)")
	flag.Parse()

	if port := os.Getenv("GRPC_PORT"); port != "" {
		*addr = ":" + port
	}

	retention, err := time.ParseDuration(*retentionStr)
	if err != nil {
		log.Fatalf("metric-server: invalid -retention %q: %v", *retentionStr, err)
	}

	// --- Singleton: store ---
	ts, err := store.New(*dbPath, retention)
	if err != nil {
		log.Fatalf("metric-server: open store: %v", err)
	}
	defer ts.Close()

	// --- Singleton: aggregation service (in-process, not over the network) ---
	aggSvc := aggregation.New()

	// --- Executor wires store → aggregation ---
	executor := mqlpkg.NewExecutor(ts)

	// --- gRPC server ---
	var opts []grpc.ServerOption
	if *tlsEnabled {
		if *certFile == "" || *keyFile == "" {
			log.Fatal("metric-server: -cert-file and -key-file are required when -tls is set")
		}
		creds, err := credentials.NewServerTLSFromFile(*certFile, *keyFile)
		if err != nil {
			log.Fatalf("metric-server: failed to load TLS credentials: %v", err)
		}
		opts = append(opts, grpc.Creds(creds))
	}

	srv := grpc.NewServer(opts...)

	// Register all three services — one construction site, one server.
	pb.RegisterAggregationServiceServer(srv, aggSvc)
	pb.RegisterMetricServiceServer(srv, metric.New(ts))
	pb.RegisterQueryServiceServer(srv, query.New(executor))

	reflection.Register(srv)

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("metric-server: failed to listen on %s: %v", *addr, err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Background retention purge — runs once per hour.
	go func() {
		ticker := time.NewTicker(time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := ts.Purge(context.Background()); err != nil {
					log.Printf("metric-server: purge: %v", err)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		log.Printf("metric-server: listening on %s (db=%s, retention=%s, tls=%v)",
			*addr, *dbPath, retention, *tlsEnabled)
		if err := srv.Serve(lis); err != nil {
			log.Fatalf("metric-server: serve: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("metric-server: shutting down...")

	drained := make(chan struct{})
	go func() {
		srv.GracefulStop()
		close(drained)
	}()

	select {
	case <-drained:
		log.Println("metric-server: clean shutdown")
	case <-time.After(10 * time.Second):
		log.Println("metric-server: drain timeout exceeded, forcing stop")
		srv.Stop()
	}
}
