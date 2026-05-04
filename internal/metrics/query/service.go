// Package query implements the QueryService gRPC server.
// Clients send an MQL query string and receive a server-streaming response of
// aligned, reduced time series.
package query

import (
	"fmt"
	"log"

	"google.golang.org/grpc"

	"github.com/accretional/grpc-server-config/internal/metrics/mql"
	pb "github.com/accretional/grpc-server-config/pb/metrics"
)

// Service implements pb.QueryServiceServer.
type Service struct {
	pb.UnimplementedQueryServiceServer
	executor *mql.Executor
}

// New returns a QueryService backed by the given Executor.
func New(executor *mql.Executor) *Service {
	return &Service{executor: executor}
}

// Query parses the MQL query, executes it, and streams one QueryResponse per
// output series back to the client.
func (s *Service) Query(req *pb.QueryRequest, stream grpc.ServerStreamingServer[pb.QueryResponse]) error {
	if req.MqlQuery == "" {
		return fmt.Errorf("query: mql_query must not be empty")
	}

	plan, err := mql.Parse(req.MqlQuery)
	if err != nil {
		return fmt.Errorf("query: parse: %w", err)
	}

	result, err := s.executor.Execute(stream.Context(), plan)
	if err != nil {
		return fmt.Errorf("query: execute: %w", err)
	}

	log.Printf("query: sending %d series for %q", len(result.Series), req.MqlQuery)

	for _, series := range result.Series {
		if err := stream.Send(&pb.QueryResponse{Series: series}); err != nil {
			return fmt.Errorf("query: send: %w", err)
		}
	}
	return nil
}

// Ensure Service satisfies the interface at compile time.
var _ pb.QueryServiceServer = (*Service)(nil)
