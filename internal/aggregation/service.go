package aggregation

import (
	"context"

	pb "github.com/accretional/grpc-server-config/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Service implements pb.AggregationServiceServer.
type Service struct{}

func New() *Service { return &Service{} }

func (s *Service) Align(ctx context.Context, req *pb.AlignRequest) (*pb.AlignResponse, error) {
	if req.Input == nil {
		return nil, status.Error(codes.InvalidArgument, "input is required")
	}
	if req.Aligner == pb.Aligner_ALIGN_NONE {
		return &pb.AlignResponse{Output: req.Input}, nil
	}
	period := req.AlignmentPeriod.AsDuration()
	if period <= 0 {
		return nil, status.Error(codes.InvalidArgument, "alignment_period must be > 0 for all aligners except ALIGN_NONE")
	}
	out, err := alignSeries(req.Input, req.Aligner, period)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	return &pb.AlignResponse{Output: out}, nil
}

func (s *Service) Reduce(ctx context.Context, req *pb.ReduceRequest) (*pb.ReduceResponse, error) {
	if req.Reducer == pb.Reducer_REDUCE_NONE || len(req.Series) == 0 {
		return &pb.ReduceResponse{Series: req.Series}, nil
	}
	out, err := reduceSeries(req.Series, req.Reducer, req.GroupByFields)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	return &pb.ReduceResponse{Series: out}, nil
}
