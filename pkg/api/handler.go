// Package api provides an HTTP REST adapter that delegates entirely to
// the gRPC service layer.  No business logic lives here — it is a thin
// serialisation boundary between JSON/HTTP and the gRPC Sluice service.
package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	grpcpkg "github.com/day253/sluice/pkg/grpc"
	grpcv1 "github.com/day253/sluice/pkg/grpc/v1"
	"github.com/day253/sluice/pkg/metrics"
	raftpkg "github.com/day253/sluice/pkg/raft"
	"github.com/day253/sluice/pkg/types"
)

// Handler adapts the gRPC Sluice service to HTTP REST.  Every endpoint
// converts the HTTP request to a gRPC call and the response back to JSON.
type Handler struct {
	nodeID             string
	svc                *grpcpkg.Service
	joinFunc           func(nodeID, raftAddr, httpAddr string, workers int) error
	workerRegisterFunc func(types.NodeInfo) error
	workerCapacityFunc func(context.Context, string, int) (types.WorkerCapacityResponse, error)
	raftStatusFunc     func() (raftpkg.MembershipStatus, error)
	performanceFunc    func(context.Context, bool, bool) (metrics.PerformanceDiagnostics, error)
	collector          interface {
		Query(name, includePrefix, excludePrefix string) ([]MetricsData, int)
	}
	logger *zap.Logger
}

type MetricsData struct {
	Name   string            `json:"name"`
	Labels map[string]string `json:"labels,omitempty"`
	Secs   []int64           `json:"secs"`
	Mins   []int64           `json:"mins"`
	Hours  []int64           `json:"hours"`
	Days   []int64           `json:"days"`
}

// NewHandler creates an HTTP handler backed by the given gRPC service.
func NewHandler(nodeID string, svc *grpcpkg.Service, logger *zap.Logger) *Handler {
	return &Handler{nodeID: nodeID, svc: svc, logger: logger}
}

// SetCollector sets the metrics collector for /api/v1/metrics endpoint.
func (h *Handler) SetCollector(c interface {
	Query(name, includePrefix, excludePrefix string) ([]MetricsData, int)
}) {
	h.collector = c
}

// SetJoinFunc configures the handler to handle cluster-join requests.
func (h *Handler) SetJoinFunc(fn func(nodeID, raftAddr, httpAddr string, workers int) error) {
	h.joinFunc = fn
}

// SetWorkerRegisterFunc configures stateless execution-node registration.
// Unlike cluster join, this never changes Raft membership.
func (h *Handler) SetWorkerRegisterFunc(fn func(types.NodeInfo) error) {
	h.workerRegisterFunc = fn
}

// SetWorkerCapacityFunc configures a Raft-backed effective concurrency update
// for one stateless Worker instance.
func (h *Handler) SetWorkerCapacityFunc(
	fn func(context.Context, string, int) (types.WorkerCapacityResponse, error),
) {
	h.workerCapacityFunc = fn
}

// SetRaftStatusFunc configures the read-only consensus membership endpoint.
func (h *Handler) SetRaftStatusFunc(fn func() (raftpkg.MembershipStatus, error)) {
	h.raftStatusFunc = fn
}

// SetPerformanceFunc configures leader-local performance diagnostics. The
// first boolean is true for an internal local-only proxy request and prevents
// loops if leadership changes while a follower is forwarding the read. The
// second boolean controls whether the bounded histories are included.
func (h *Handler) SetPerformanceFunc(fn func(context.Context, bool, bool) (metrics.PerformanceDiagnostics, error)) {
	h.performanceFunc = fn
}

// RegisterRoutes attaches all endpoints to the given router.
func (h *Handler) RegisterRoutes(r *mux.Router) {
	r.HandleFunc("/api/v1/tasks", h.submitTask).Methods("POST")
	r.HandleFunc("/api/v1/tasks/batch", h.submitBatch).Methods("POST")
	r.HandleFunc("/api/v1/tasks/{task_id}", h.getTask).Methods("GET")
	r.HandleFunc("/api/v1/tasks/{task_id}/wait", h.waitTask).Methods("GET")

	r.HandleFunc("/api/v1/admin/tenants", h.listTenants).Methods("GET")
	r.HandleFunc("/api/v1/admin/tenants/{tenant_id}", h.upsertTenant).Methods("PUT")
	r.HandleFunc("/api/v1/admin/tenants/{tenant_id}", h.deleteTenant).Methods("DELETE")

	r.HandleFunc("/api/v1/admin/nodes", h.listNodes).Methods("GET")
	r.HandleFunc("/api/v1/admin/nodes/{node_id}/capacity", h.setWorkerCapacity).Methods("PUT")
	r.HandleFunc("/api/v1/admin/allocations", h.getAllocations).Methods("GET")
	r.HandleFunc("/api/v1/admin/raft", h.raftStatus).Methods("GET")
	r.HandleFunc("/api/v1/admin/performance", h.performance).Methods("GET")

	r.HandleFunc("/api/v1/cluster/join", h.joinCluster).Methods("POST")
	r.HandleFunc("/api/v1/cluster/workers/register", h.registerWorker).Methods("POST")
	r.HandleFunc("/api/v1/metrics", h.metrics).Methods("GET")
	r.HandleFunc("/api/v1/metrics/{name}", h.metrics).Methods("GET")
	r.HandleFunc("/api/v1/health", h.health).Methods("GET")
}

func (h *Handler) raftStatus(w http.ResponseWriter, _ *http.Request) {
	if h.raftStatusFunc == nil {
		h.writeError(w, http.StatusInternalServerError, "raft status not configured")
		return
	}
	status, err := h.raftStatusFunc()
	if err != nil {
		h.writeError(w, http.StatusServiceUnavailable, "raft status unavailable: "+err.Error())
		return
	}
	h.writeJSON(w, http.StatusOK, status)
}

func (h *Handler) performance(w http.ResponseWriter, r *http.Request) {
	if h.performanceFunc == nil {
		h.writeError(w, http.StatusInternalServerError, "performance diagnostics not configured")
		return
	}
	diagnostics, err := h.performanceFunc(
		r.Context(),
		r.URL.Query().Get("local") == "1",
		r.URL.Query().Get("history") != "0",
	)
	if err != nil {
		h.writeError(w, http.StatusServiceUnavailable, "performance diagnostics unavailable: "+err.Error())
		return
	}
	h.writeJSON(w, http.StatusOK, diagnostics)
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

	resp, err := h.svc.Submit(r.Context(), &grpcv1.SubmitRequest{
		TenantId: req.TenantID, Payload: req.Payload,
		IdempotencyKey: req.IdempotencyKey,
	})
	if err != nil {
		h.writeGRPCError(w, err)
		return
	}

	h.writeJSON(w, http.StatusAccepted, types.TaskResponse{
		TaskID: resp.TaskId, TenantID: resp.TenantId, Status: resp.Status,
	})
}

func (h *Handler) submitBatch(w http.ResponseWriter, r *http.Request) {
	var body types.BatchTaskSubmitRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid body: "+err.Error())
		return
	}
	req := &grpcv1.SubmitBatchRequest{Tasks: make([]*grpcv1.SubmitRequest, len(body.Tasks))}
	for i, task := range body.Tasks {
		req.Tasks[i] = &grpcv1.SubmitRequest{
			TenantId: task.TenantID, Payload: task.Payload,
			IdempotencyKey: task.IdempotencyKey,
		}
	}
	resp, err := h.svc.SubmitBatch(r.Context(), req)
	if err != nil {
		h.writeGRPCError(w, err)
		return
	}
	out := types.BatchTaskResponse{Tasks: make([]types.TaskResponse, len(resp.Tasks))}
	for i, task := range resp.Tasks {
		out.Tasks[i] = types.TaskResponse{TaskID: task.TaskId, TenantID: task.TenantId, Status: task.Status}
	}
	h.writeJSON(w, http.StatusAccepted, out)
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

	resp, err := h.svc.WaitTask(r.Context(), &grpcv1.WaitTaskRequest{
		TaskId: taskID, TimeoutSeconds: timeout,
	})
	if err != nil {
		h.writeGRPCError(w, err)
		return
	}
	h.writeJSON(w, http.StatusOK, types.TaskResponse{
		TaskID: resp.TaskId, TenantID: resp.TenantId,
		Status: resp.Status, Result: resp.Result, Error: resp.Error,
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
	// Return with inflight count included.
	out := make(map[string]map[string]interface{}, len(resp.Tenants))
	for _, t := range resp.Tenants {
		out[t.TenantId] = map[string]interface{}{
			"id":          t.TenantId,
			"name":        t.Name,
			"max_workers": t.MaxWorkers,
			"inflight":    t.Inflight,
		}
	}
	h.writeJSON(w, http.StatusOK, out)
}

// ---------------------------------------------------------------------------
// Admin — cluster
// ---------------------------------------------------------------------------

func (h *Handler) listNodes(w http.ResponseWriter, r *http.Request) {
	nodes, leader := h.svc.NodeSnapshot()
	type nodeView struct {
		NodeID           string `json:"node_id"`
		Role             string `json:"role,omitempty"`
		Address          string `json:"address"`
		RaftAddress      string `json:"raft_address,omitempty"`
		Status           string `json:"status"`
		TotalWorkers     int    `json:"total_workers"`
		CapacityOverride int    `json:"capacity_override,omitempty"`
	}
	views := make([]nodeView, 0, len(nodes))
	for _, node := range nodes {
		views = append(views, nodeView{
			NodeID: node.ID, Role: node.Role, Address: node.Address,
			RaftAddress: node.RaftAddress, Status: node.Status, TotalWorkers: node.TotalWorkers,
			CapacityOverride: node.CapacityOverride,
		})
	}
	h.writeJSON(w, http.StatusOK, map[string]interface{}{
		"leader_address": leader,
		"nodes":          views,
	})
}

func (h *Handler) setWorkerCapacity(w http.ResponseWriter, r *http.Request) {
	var req struct {
		TotalWorkers int `json:"total_workers"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid body: "+err.Error())
		return
	}
	if req.TotalWorkers < 1 || req.TotalWorkers > types.MaxWorkerCapacityPerInstance {
		h.writeError(
			w,
			http.StatusBadRequest,
			"total_workers must be between 1 and "+
				strconv.Itoa(types.MaxWorkerCapacityPerInstance),
		)
		return
	}
	nodeID := mux.Vars(r)["node_id"]
	nodes, _ := h.svc.NodeSnapshot()
	node := nodes[nodeID]
	if node == nil {
		h.writeError(w, http.StatusNotFound, "worker node not found")
		return
	}
	if node.Role != types.NodeRoleWorker {
		h.writeError(w, http.StatusBadRequest, "capacity can only be configured for worker nodes")
		return
	}
	if node.Status != types.NodeStatusUp {
		h.writeError(w, http.StatusConflict, "worker node is not live")
		return
	}
	if h.workerCapacityFunc == nil {
		h.writeError(w, http.StatusInternalServerError, "worker capacity update not configured")
		return
	}
	response, err := h.workerCapacityFunc(r.Context(), nodeID, req.TotalWorkers)
	if err != nil {
		h.writeError(w, http.StatusServiceUnavailable, "worker capacity update failed: "+err.Error())
		return
	}
	h.writeJSON(w, http.StatusOK, response)
}

func (h *Handler) getAllocations(w http.ResponseWriter, r *http.Request) {
	allocations, tenants := h.svc.AllocationSnapshot()
	nodes := make([]*types.NodeAllocation, 0, len(allocations))
	for _, allocation := range allocations {
		nodes = append(nodes, allocation)
	}
	h.writeJSON(w, http.StatusOK, types.AllocationResponse{Nodes: nodes, Tenants: tenants})
}

// ---------------------------------------------------------------------------
// Join / Health
// ---------------------------------------------------------------------------

func (h *Handler) joinCluster(w http.ResponseWriter, r *http.Request) {
	var req struct {
		NodeID       string `json:"node_id"`
		RaftAddress  string `json:"raft_address"`
		HTTPAddress  string `json:"http_address"`
		TotalWorkers int    `json:"total_workers"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid body: "+err.Error())
		return
	}
	if req.NodeID == "" || req.RaftAddress == "" {
		h.writeError(w, http.StatusBadRequest, "node_id and raft_address required")
		return
	}
	if h.joinFunc == nil {
		h.writeError(w, http.StatusInternalServerError, "join not configured")
		return
	}
	if err := h.joinFunc(req.NodeID, req.RaftAddress, req.HTTPAddress, req.TotalWorkers); err != nil {
		h.writeError(w, http.StatusInternalServerError, "join failed: "+err.Error())
		return
	}
	h.writeJSON(w, http.StatusOK, map[string]bool{"success": true})
}

func (h *Handler) registerWorker(w http.ResponseWriter, r *http.Request) {
	var req struct {
		NodeID       string `json:"node_id"`
		SessionID    string `json:"session_id"`
		HTTPAddress  string `json:"http_address"`
		TotalWorkers int    `json:"total_workers"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid body: "+err.Error())
		return
	}
	if req.NodeID == "" || req.SessionID == "" || req.TotalWorkers < 1 {
		h.writeError(w, http.StatusBadRequest, "node_id, session_id and positive total_workers required")
		return
	}
	if h.workerRegisterFunc == nil {
		h.writeError(w, http.StatusInternalServerError, "worker registration not configured")
		return
	}
	if err := h.workerRegisterFunc(types.NodeInfo{
		ID: req.NodeID, Role: types.NodeRoleWorker, SessionID: req.SessionID,
		Address: req.HTTPAddress, Status: types.NodeStatusUp, TotalWorkers: req.TotalWorkers,
	}); err != nil {
		h.writeError(w, http.StatusServiceUnavailable, "worker registration failed: "+err.Error())
		return
	}
	h.writeJSON(w, http.StatusOK, map[string]bool{"success": true})
}

func (h *Handler) metrics(w http.ResponseWriter, r *http.Request) {
	if h.collector == nil {
		h.writeJSON(w, http.StatusOK, []interface{}{})
		return
	}
	name := mux.Vars(r)["name"]
	includePrefix := r.URL.Query().Get("prefix")
	excludePrefix := ""
	if r.URL.Query().Get("performance") == "0" {
		excludePrefix = "performance:"
	}
	data, _ := h.collector.Query(name, includePrefix, excludePrefix)
	h.writeJSON(w, http.StatusOK, data)
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
