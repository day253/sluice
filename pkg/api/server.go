package api

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"go.uber.org/zap"
)

// Server is the HTTP API server.
type Server struct {
	srv    *http.Server
	router *mux.Router
	logger *zap.Logger
}

// NewServer creates a configured HTTP server (not yet running).
func NewServer(addr string, handler *Handler, logger *zap.Logger) *Server {
	router := mux.NewRouter()
	router.Use(loggingMiddleware(logger))
	router.Use(recoveryMiddleware(logger))

	handler.RegisterRoutes(router)

	return &Server{
		srv: &http.Server{
			Addr:         addr,
			Handler:      router,
			ReadTimeout:  15 * time.Second,
			WriteTimeout: 60 * time.Second,
			IdleTimeout:  120 * time.Second,
		},
		router: router,
		logger: logger,
	}
}

// NewRouter creates a configured HTTP router for use with cmux or httptest.
func NewRouter(handler *Handler) *mux.Router {
	router := mux.NewRouter()
	router.Use(loggingMiddleware(zap.NewNop()))
	router.Use(recoveryMiddleware(zap.NewNop()))
	handler.RegisterRoutes(router)
	return router
}

// Start begins listening and serving.  It blocks until the server is stopped
// via Shutdown or an error occurs.
func (s *Server) Start() error {
	s.logger.Info("api: starting HTTP server", zap.String("addr", s.srv.Addr))
	if err := s.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("http serve: %w", err)
	}
	return nil
}

// Shutdown gracefully stops the server with the given deadline.
func (s *Server) Shutdown(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	s.logger.Info("api: shutting down HTTP server")
	return s.srv.Shutdown(ctx)
}

// Router returns the mux router (useful for testing).
func (s *Server) Router() *mux.Router {
	return s.router
}

// ---------------------------------------------------------------------------
// Middleware
// ---------------------------------------------------------------------------

func loggingMiddleware(logger *zap.Logger) mux.MiddlewareFunc {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			next.ServeHTTP(w, r)
			logger.Debug("http request",
				zap.String("method", r.Method),
				zap.String("path", r.URL.Path),
				zap.Duration("duration", time.Since(start)),
			)
		})
	}
}

func recoveryMiddleware(logger *zap.Logger) mux.MiddlewareFunc {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					logger.Error("http panic recovered",
						zap.String("path", r.URL.Path),
						zap.Any("panic", rec),
					)
					http.Error(w, `{"error":"internal server error","code":500}`, http.StatusInternalServerError)
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}
