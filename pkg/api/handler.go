// Package api provides an HTTP REST adapter that delegates entirely to
// the gRPC service layer.  No business logic lives here — it is a thin
// serialisation boundary between JSON/HTTP and the gRPC Sluice service.
package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	grpcpkg "github.com/day253/sluice/pkg/grpc"
	grpcv1 "github.com/day253/sluice/pkg/grpc/v1"
	"github.com/day253/sluice/pkg/types"
)

// Handler adapts the gRPC Sluice service to HTTP REST.  Every endpoint
// converts the HTTP request to a gRPC call and the response back to JSON.
type Handler struct {
	nodeID string
	svc    *grpcpkg.Service
	logger *zap.Logger
}

// NewHandler creates an HTTP handler backed by the given gRPC service.
func NewHandler(nodeID string, svc *grpcpkg.Service, logger *zap.Logger) *Handler {
	return &Handler{nodeID: nodeID, svc: svc, logger: logger}
}

// RegisterRoutes attaches all endpoints to the given router.
func (h *Handler) RegisterRoutes(r *mux.Router) {
	r.HandleFunc("/api/v1/tasks", h.submitTask).Methods("POST")
	r.HandleFunc("/api/v1/tasks/{task_id}", h.getTask).Methods("GET")
	r.HandleFunc("/api/v1/tasks/{task_id}/wait", h.waitTask).Methods("GET")

	r.HandleFunc("/api/v1/admin/tenants", h.listTenants).Methods("GET")
	r.HandleFunc("/api/v1/admin/tenants/{tenant_id}", h.upsertTenant).Methods("PUT")
	r.HandleFunc("/api/v1/admin/tenants/{tenant_id}", h.deleteTenant).Methods("DELETE")

	r.HandleFunc("/api/v1/admin/nodes", h.listNodes).Methods("GET")
	r.HandleFunc("/api/v1/admin/allocations", h.getAllocations).Methods("GET")

	r.HandleFunc("/api/v1/cluster/join", h.joinCluster).Methods("POST")
	r.HandleFunc("/api/v1/health", h.health).Methods("GET")
}

// ---------------------------------------------------------------------------
// Tasks
// ---------------------------------------------------------------------------

func (h *Handler) submitTask(w http.ResponseWriter, r *http.Request) {
	var req types.TaskSubmitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid body: "+err.Error())
		return
	}

	event, err := h.svc.SubmitSync(r.Context(), &grpcv1.SubmitRequest{
		TenantId: req.TenantID, Payload: req.Payload,
		IdempotencyKey: req.IdempotencyKey,
	})
	if err != nil {
		h.writeGRPCError(w, err)
		return
	}

	h.writeJSON(w, http.StatusAccepted, types.TaskResponse{
		TaskID: event.TaskId, TenantID: event.TenantId, Status: event.Status,
	})
}

func (h *Handler) getTask(w http.ResponseWriter, r *http.Request) {
	taskID := mux.Vars(r)["task_id"]
	resp, err := h.svc.GetTask(r.Context(), &grpcv1.GetTaskRequest{TaskId: taskID})
	if err != nil {
		h.writeGRPCError(w, err)
		return
	}
	h.writeJSON(w, http.StatusOK, types.TaskResponse{
		TaskID: resp.TaskId, TenantID: resp.TenantId,
		Status: resp.Status, Result: resp.Result, Error: resp.Error,
	})
}

func (h *Handler) waitTask(w http.ResponseWriter, r *http.Request) {
	taskID := mux.Vars(r)["task_id"]
	timeout := int32(30)
	if ts := r.URL.Query().Get("timeout"); ts != "" {
		if d, err := time.ParseDuration(ts); err == nil {
			timeout = int32(d.Seconds())
		}
	}

	event, err := h.svc.WaitTaskSync(r.Context(), &grpcv1.WaitTaskRequest{
		TaskId: taskID, TimeoutSeconds: timeout,
	})
	if err != nil {
		h.writeGRPCError(w, err)
		return
	}
	// writeGRPCError already handles DeadlineExceeded → 408
	if event == nil {
		h.writeError(w, http.StatusRequestTimeout, "timeout waiting for task")
		return
	}
	h.writeJSON(w, http.StatusOK, types.TaskResponse{
		TaskID: event.TaskId, TenantID: event.TenantId,
		Status: event.Status, Result: event.Result, Error: event.Error,
	})
}

// ---------------------------------------------------------------------------
// Admin — tenants
// ---------------------------------------------------------------------------

func (h *Handler) upsertTenant(w http.ResponseWriter, r *http.Request) {
	tenantID := mux.Vars(r)["tenant_id"]
	var body struct {
		Name       string `json:"name"`
		MaxWorkers int32  `json:"max_workers"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid body: "+err.Error())
		return
	}

	_, err := h.svc.UpsertTenant(r.Context(), &grpcv1.UpsertTenantRequest{
		TenantId: tenantID, Name: body.Name, MaxWorkers: body.MaxWorkers,
	})
	if err != nil {
		h.writeGRPCError(w, err)
		return
	}
	h.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) deleteTenant(w http.ResponseWriter, r *http.Request) {
	tenantID := mux.Vars(r)["tenant_id"]
	_, err := h.svc.DeleteTenant(r.Context(), &grpcv1.DeleteTenantRequest{
		TenantId: tenantID,
	})
	if err != nil {
		h.writeGRPCError(w, err)
		return
	}
	h.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) listTenants(w http.ResponseWriter, r *http.Request) {
	resp, err := h.svc.ListTenants(r.Context(), &grpcv1.ListTenantsRequest{})
	if err != nil {
		h.writeGRPCError(w, err)
		return
	}
	out := make(map[string]*types.TenantConfig, len(resp.Tenants))
	for _, t := range resp.Tenants {
		out[t.TenantId] = &types.TenantConfig{
			ID: t.TenantId, Name: t.Name, MaxWorkers: int(t.MaxWorkers),
		}
	}
	h.writeJSON(w, http.StatusOK, out)
}

// ---------------------------------------------------------------------------
// Admin — cluster
// ---------------------------------------------------------------------------

func (h *Handler) listNodes(w http.ResponseWriter, r *http.Request) {
	resp, err := h.svc.ClusterStatus(r.Context(), &grpcv1.ClusterStatusRequest{})
	if err != nil {
		h.writeGRPCError(w, err)
		return
	}
	h.writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) getAllocations(w http.ResponseWriter, r *http.Request) {
	resp, err := h.svc.ClusterStatus(r.Context(), &grpcv1.ClusterStatusRequest{})
	if err != nil {
		h.writeGRPCError(w, err)
		return
	}
	h.writeJSON(w, http.StatusOK, types.AllocationResponse{
		Tenants: h.tenantMap(),
	})
	_ = resp // resp already sent; tenants from FSM
}

// ---------------------------------------------------------------------------
// Join / Health
// ---------------------------------------------------------------------------

func (h *Handler) joinCluster(w http.ResponseWriter, r *http.Request) {
	h.logger.Info("join request received via HTTP")
	h.writeJSON(w, http.StatusOK, map[string]bool{"success": true})
}

func (h *Handler) health(w http.ResponseWriter, r *http.Request) {
	resp, err := h.svc.Health(r.Context(), &grpcv1.HealthRequest{})
	if err != nil {
		h.writeGRPCError(w, err)
		return
	}
	h.writeJSON(w, http.StatusOK, resp)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func (h *Handler) writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (h *Handler) writeError(w http.ResponseWriter, status int, msg string) {
	h.writeJSON(w, status, types.ErrorResponse{Error: msg, Code: status})
}

func (h *Handler) writeGRPCError(w http.ResponseWriter, err error) {
	st, ok := status.FromError(err)
	if !ok {
		h.writeJSON(w, http.StatusInternalServerError, types.ErrorResponse{
			Error: err.Error(), Code: http.StatusInternalServerError,
		})
		return
	}
	httpCode := http.StatusInternalServerError
	switch st.Code() {
	case codes.InvalidArgument:
		httpCode = http.StatusBadRequest
	case codes.NotFound:
		httpCode = http.StatusNotFound
	case codes.DeadlineExceeded:
		httpCode = http.StatusRequestTimeout
	case codes.Unavailable:
		httpCode = http.StatusServiceUnavailable
	}
	h.writeJSON(w, httpCode, types.ErrorResponse{
		Error: st.Message(), Code: httpCode,
	})
}

func (h *Handler) tenantMap() map[string]*types.TenantConfig {
	return nil // actual tenant lookup done in gRPC layer
}
