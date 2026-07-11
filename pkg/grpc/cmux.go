package grpc

import (
	"fmt"
	"net"
	"net/http"

	"github.com/soheilhy/cmux"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	grpcv1 "github.com/day253/sluice/pkg/grpc/v1"
)

// MultiplexServer serves HTTP REST + gRPC (external + internal) on a
// single TCP port using cmux connection multiplexing.
//
//	HTTP/1.1           → REST handler
//	gRPC (HTTP/2)      → grpc.Server (Sluice + SluiceInternal)
type MultiplexServer struct {
	addr    string
	mux     cmux.CMux
	httpSrv *http.Server
	grpcSrv *grpc.Server
	logger  *zap.Logger
}

// NewMultiplexServer creates the multiplexed server.  Both the external
// Sluice service and internal SluiceInternal service are registered on
// the same gRPC server (distinct method names, no conflict).
func NewMultiplexServer(
	addr string,
	httpHandler http.Handler,
	svc grpcv1.SluiceServer,
	internalSvc grpcv1.SluiceInternalServer,
	logger *zap.Logger,
) (*MultiplexServer, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("cmux listen %s: %w", addr, err)
	}

	m := cmux.New(lis)

	// HTTP/1.1 matcher.
	httpLis := m.Match(cmux.HTTP1Fast())
	// Everything else → gRPC.
	grpcLis := m.Match(cmux.Any())

	grpcSrv := grpc.NewServer(grpc.MaxConcurrentStreams(4096))
	grpcv1.RegisterSluiceServer(grpcSrv, svc)
	if internalSvc != nil {
		grpcv1.RegisterSluiceInternalServer(grpcSrv, internalSvc)
	}
	reflection.Register(grpcSrv)

	httpSrv := &http.Server{Handler: httpHandler}

	// Start sub-servers immediately (they block on the matched listeners).
	go func() { _ = httpSrv.Serve(httpLis) }()
	go func() { _ = grpcSrv.Serve(grpcLis) }()

	return &MultiplexServer{
		addr:    addr,
		mux:     m,
		httpSrv: httpSrv,
		grpcSrv: grpcSrv,
		logger:  logger,
	}, nil
}

// Start begins multiplexing.  Blocks until Stop is called.
func (ms *MultiplexServer) Start() error {
	ms.logger.Info("cmux: serving HTTP+gRPC on single port",
		zap.String("addr", ms.addr),
	)
	return ms.mux.Serve()
}

// Stop gracefully shuts down all services.
func (ms *MultiplexServer) Stop() {
	ms.logger.Info("cmux: stopping")
	ms.grpcSrv.GracefulStop()
	_ = ms.httpSrv.Close()
}
