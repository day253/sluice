package loadgen

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"go.uber.org/zap"
)

type Server struct {
	server  *http.Server
	manager *Manager
	logger  *zap.Logger
}

func NewServer(address string, manager *Manager, logger *zap.Logger) *Server {
	return &Server{
		server: &http.Server{
			Addr: address, Handler: NewHandler(manager),
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       15 * time.Second,
			WriteTimeout:      15 * time.Second,
			IdleTimeout:       120 * time.Second,
		},
		manager: manager, logger: logger,
	}
}

func NewHandler(manager *Manager) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"status": "ok", "role": "load-generator",
		})
	})
	mux.HandleFunc("/api/v1/load-runs", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var request StartRequest
		r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&request); err != nil {
			writeError(w, http.StatusBadRequest, "invalid load parameters: "+err.Error())
			return
		}
		if err := decoder.Decode(&struct{}{}); err != io.EOF {
			writeError(w, http.StatusBadRequest, "invalid load parameters: exactly one JSON object is required")
			return
		}
		run, err := manager.Start(request)
		if err != nil {
			if errors.Is(err, ErrRunActive) {
				writeError(w, http.StatusConflict, err.Error())
				return
			}
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]any{"run": run})
	})
	mux.HandleFunc("/api/v1/load-runs/current", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		run, ok := manager.Current()
		if !ok {
			writeJSON(w, http.StatusOK, map[string]any{"run": nil})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"run": run})
	})
	mux.HandleFunc("/api/v1/load-runs/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		id := strings.TrimPrefix(r.URL.Path, "/api/v1/load-runs/")
		if id == "" || strings.Contains(id, "/") {
			writeError(w, http.StatusNotFound, "load generation run not found")
			return
		}
		run, err := manager.Stop(id)
		if err != nil {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"run": run})
	})
	return mux
}

func (s *Server) Start() error {
	s.logger.Info("load generator: starting HTTP server", zap.String("addr", s.server.Addr))
	if err := s.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("load generator HTTP serve: %w", err)
	}
	return nil
}

func (s *Server) Shutdown(timeout time.Duration) error {
	s.manager.Close()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return s.server.Shutdown(ctx)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]any{"error": message, "code": status})
}
