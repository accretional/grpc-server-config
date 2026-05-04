// Package metric implements the MetricService gRPC server.
// Agents call WriteTimeSeries to push time series data into the store.
package metric

import (
	"fmt"
	"io"
	"log"

	"google.golang.org/grpc"

	"github.com/accretional/grpc-server-config/internal/metrics/store"
	pb "github.com/accretional/grpc-server-config/pb/metrics"
)

// Service implements pb.MetricServiceServer.
type Service struct {
	pb.UnimplementedMetricServiceServer
	store *store.Store
}

// New returns a MetricService backed by the given store.
func New(s *store.Store) *Service {
	return &Service{store: s}
}

// WriteTimeSeries receives a client-streaming sequence of batches, writes each
// to the store, and returns the total number of points persisted.
func (s *Service) WriteTimeSeries(stream grpc.ClientStreamingServer[pb.WriteTimeSeriesRequest, pb.WriteTimeSeriesResponse]) error {
	ctx := stream.Context()
	total := 0

	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("metric: recv: %w", err)
		}

		n, err := s.store.Write(ctx, req.Series)
		if err != nil {
			return fmt.Errorf("metric: write: %w", err)
		}
		total += n
		log.Printf("metric: wrote %d points (total this stream: %d)", n, total)
	}

	return stream.SendAndClose(&pb.WriteTimeSeriesResponse{PointsWritten: int32(total)})
}

// Ensure Service satisfies the interface at compile time.
var _ pb.MetricServiceServer = (*Service)(nil)
