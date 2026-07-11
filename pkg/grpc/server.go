package grpc

import (
	"fmt"
	"net"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	grpcv1 "github.com/day253/sluice/pkg/grpc/v1"
)

// Server wraps a gRPC server with the Sluice service registered.
type Server struct {
	listener net.Listener
	srv      *grpc.Server
	logger   *zap.Logger
	addr     string
}

// NewServer creates an unstarted gRPC server with the Sluice service
// registered and reflection enabled (useful for grpcurl debugging).
func NewServer(addr string, svc *Service, logger *zap.Logger) (*Server, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("grpc listen %s: %w", addr, err)
	}

	srv := grpc.NewServer(
		grpc.MaxConcurrentStreams(1024),
	)

	grpcv1.RegisterSluiceServer(srv, svc)
	reflection.Register(srv) // enables grpcurl / grpcui

	return &Server{
		listener: lis,
		srv:      srv,
		logger:   logger,
		addr:     addr,
	}, nil
}

// Start begins serving.  Blocks until Stop is called or the listener fails.
func (s *Server) Start() error {
	s.logger.Info("grpc: starting", zap.String("addr", s.addr))
	return s.srv.Serve(s.listener)
}

// Stop gracefully shuts down the gRPC server.
func (s *Server) Stop() {
	s.logger.Info("grpc: stopping")
	s.srv.GracefulStop()
}

// Addr returns the listening address.
func (s *Server) Addr() string {
	return s.addr
}
