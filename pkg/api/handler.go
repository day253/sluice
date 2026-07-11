package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"go.uber.org/zap"

	raftpkg "github.com/day253/sluice/pkg/raft"
	"github.com/day253/sluice/pkg/queue"
	"github.com/day253/sluice/pkg/types"
	"github.com/day253/sluice/pkg/worker"
)

// Handler implements the HTTP API for task submission, status queries, and
// administrative operations.
type Handler struct {
	nodeID    string
	queue     queue.Queue
	fsm       *raftpkg.FSM
	raft      raftpkg.RaftApplier
	pool      *worker.Pool
	logger    *zap.Logger
}

// NewHandler creates an API handler with injected dependencies.
func NewHandler(
	nodeID string,
	q queue.Queue,
	fsm *raftpkg.FSM,
	raft raftpkg.RaftApplier,
	pool *worker.Pool,
	logger *zap.Logger,
) *Handler {
	return &Handler{
		nodeID: nodeID,
		queue:  q,
		fsm:    fsm,
		raft:   raft,
		pool:   pool,
		logger: logger,
	}
}

// RegisterRoutes attaches all endpoints to the given router.
func (h *Handler) RegisterRoutes(r *mux.Router) {
	// Task endpoints.
	r.HandleFunc("/api/v1/tasks", h.submitTask).Methods("POST")
	r.HandleFunc("/api/v1/tasks/{task_id}", h.getTask).Methods("GET")
	r.HandleFunc("/api/v1/tasks/{task_id}/wait", h.waitTask).Methods("GET")

	// Admin — tenants.
	r.HandleFunc("/api/v1/admin/tenants", h.listTenants).Methods("GET")
	r.HandleFunc("/api/v1/admin/tenants/{tenant_id}", h.upsertTenant).Methods("PUT")
	r.HandleFunc("/api/v1/admin/tenants/{tenant_id}", h.deleteTenant).Methods("DELETE")

	// Admin — cluster.
	r.HandleFunc("/api/v1/admin/nodes", h.listNodes).Methods("GET")
	r.HandleFunc("/api/v1/admin/allocations", h.getAllocations).Methods("GET")

	// Raft join endpoint.
	r.HandleFunc("/api/v1/cluster/join", h.joinCluster).Methods("POST")

	// Health.
	r.HandleFunc("/api/v1/health", h.health).Methods("GET")
}

// ---------------------------------------------------------------------------
// Task endpoints
// ---------------------------------------------------------------------------

func (h *Handler) submitTask(w http.ResponseWriter, r *http.Request) {
	var req types.TaskSubmitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}
	if req.TenantID == "" {
		h.writeError(w, http.StatusBadRequest, "tenant_id is required")
		return
	}

	// Check tenant exists.
	if _, ok := h.fsm.GetTenant(req.TenantID); !ok {
		h.writeError(w, http.StatusNotFound, "tenant not found: "+req.TenantID)
		return
	}

	taskID := uuid.New().String()
	now := time.Now().UTC()

	// Write to local durable queue.
	env := &queue.TaskEnvelope{
		TaskID:         taskID,
		TenantID:       req.TenantID,
		Payload:        req.Payload,
		IdempotencyKey: req.IdempotencyKey,
		CreatedAt:      now,
	}
	if err := h.queue.Enqueue(req.TenantID, env); err != nil {
		h.logger.Error("enqueue failed", zap.Error(err))
		h.writeError(w, http.StatusInternalServerError, "failed to enqueue task")
		return
	}

	h.logger.Info("task submitted",
		zap.String("task_id", taskID),
		zap.String("tenant", req.TenantID),
	)

	h.writeJSON(w, http.StatusAccepted, types.TaskResponse{
		TaskID:   taskID,
		TenantID: req.TenantID,
		Status:   types.TaskStatusPending,
	})
}

func (h *Handler) getTask(w http.ResponseWriter, r *http.Request) {
	taskID := mux.Vars(r)["task_id"]

	// Check inflight / recovery-pending tasks.
	if task := h.fsm.GetTask(taskID); task != nil {
		h.writeJSON(w, http.StatusOK, types.TaskResponse{
			TaskID:   task.TaskID,
			TenantID: task.TenantID,
			Status:   task.Status,
		})
		return
	}

	// Check completed results.
	if result := h.fsm.GetResult(taskID); result != nil {
		h.writeJSON(w, http.StatusOK, types.TaskResponse{
			TaskID:   result.TaskID,
			TenantID: result.TenantID,
			Status:   result.Status,
			Result:   result.Result,
			Error:    result.Error,
		})
		return
	}

	h.writeError(w, http.StatusNotFound, "task not found: "+taskID)
}

func (h *Handler) waitTask(w http.ResponseWriter, r *http.Request) {
	taskID := mux.Vars(r)["task_id"]
	timeout := 30 * time.Second
	if ts := r.URL.Query().Get("timeout"); ts != "" {
		if d, err := time.ParseDuration(ts); err == nil {
			timeout = d
		}
	}

	deadline := time.After(timeout)
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()

	for {
		// Check inflight.
		if task := h.fsm.GetTask(taskID); task != nil {
			if task.Status == types.TaskStatusPending || task.Status == types.TaskStatusInflight {
				// Still processing — wait.
			}
		}

		// Check completed.
		if result := h.fsm.GetResult(taskID); result != nil {
			h.writeJSON(w, http.StatusOK, types.TaskResponse{
				TaskID:   result.TaskID,
				TenantID: result.TenantID,
				Status:   result.Status,
				Result:   result.Result,
				Error:    result.Error,
			})
			return
		}

		select {
		case <-deadline:
			// Return current status on timeout.
			if task := h.fsm.GetTask(taskID); task != nil {
				h.writeJSON(w, http.StatusOK, types.TaskResponse{
					TaskID:   task.TaskID,
					TenantID: task.TenantID,
					Status:   task.Status,
				})
				return
			}
			h.writeError(w, http.StatusRequestTimeout, "timeout waiting for task")
			return
		case <-tick.C:
		}
	}
}

// ---------------------------------------------------------------------------
// Admin — tenants
// ---------------------------------------------------------------------------

func (h *Handler) upsertTenant(w http.ResponseWriter, r *http.Request) {
	tenantID := mux.Vars(r)["tenant_id"]

	var req struct {
		Name       string `json:"name"`
		MaxWorkers int    `json:"max_workers"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid body: "+err.Error())
		return
	}
	if req.MaxWorkers < 1 {
		h.writeError(w, http.StatusBadRequest, "max_workers must be >= 1")
		return
	}

	tc := types.TenantConfig{
		ID:         tenantID,
		Name:       req.Name,
		MaxWorkers: req.MaxWorkers,
	}
	cmd := raftpkg.MustMarshalCommand(raftpkg.OpUpsertTenant, tc)
	result := h.raft.Apply(cmd, 5000)
	if err := result.Error(); err != nil {
		h.writeError(w, http.StatusInternalServerError, "raft apply: "+err.Error())
		return
	}

	h.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) deleteTenant(w http.ResponseWriter, r *http.Request) {
	tenantID := mux.Vars(r)["tenant_id"]
	data := raftpkg.DeleteTenantData{ID: tenantID}
	cmd := raftpkg.MustMarshalCommand(raftpkg.OpDeleteTenant, data)
	result := h.raft.Apply(cmd, 5000)
	if err := result.Error(); err != nil {
		h.writeError(w, http.StatusInternalServerError, "raft apply: "+err.Error())
		return
	}
	h.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) listTenants(w http.ResponseWriter, r *http.Request) {
	tenants := h.fsm.GetAllTenants()
	h.writeJSON(w, http.StatusOK, tenants)
}

// ---------------------------------------------------------------------------
// Admin — cluster
// ---------------------------------------------------------------------------

func (h *Handler) listNodes(w http.ResponseWriter, r *http.Request) {
	nodes := h.fsm.GetAllNodes()
	status := h.pool.GetStatus()
	// Merge pool status into node list.
	resp := struct {
		Nodes  map[string]*types.NodeInfo `json:"nodes"`
		Leader string                     `json:"leader"`
		Status map[string]int             `json:"worker_status"`
	}{
		Nodes:  nodes,
		Leader: h.raft.LeaderAddr(),
		Status: status,
	}
	h.writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) getAllocations(w http.ResponseWriter, r *http.Request) {
	allocs := h.fsm.GetAllAllocations()
	tenants := h.fsm.GetAllTenants()
	h.writeJSON(w, http.StatusOK, types.AllocationResponse{
		Nodes:   mapToSlice(allocs),
		Tenants: tenants,
	})
}

// ---------------------------------------------------------------------------
// Cluster join
// ---------------------------------------------------------------------------

func (h *Handler) joinCluster(w http.ResponseWriter, r *http.Request) {
	var req raftpkg.JoinRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid join request: "+err.Error())
		return
	}

	// When a new node joins, the leader adds it as a voter and registers it.
	// This handler runs on the leader (requests are forwarded by the
	// joining node's HTTP server).
	// The actual AddVoter is done by the caller (node orchestration layer);
	// here we just confirm the join request was received.
	h.logger.Info("join request received",
		zap.String("node", req.NodeID),
		zap.String("raft_addr", req.RaftAddress),
	)

	h.writeJSON(w, http.StatusOK, raftpkg.JoinResponse{Success: true})
}

// ---------------------------------------------------------------------------
// Health
// ---------------------------------------------------------------------------

func (h *Handler) health(w http.ResponseWriter, r *http.Request) {
	h.writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":  "ok",
		"node_id": h.nodeID,
		"leader":  h.raft.LeaderAddr(),
	})
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

func mapToSlice(m map[string]*types.NodeAllocation) []*types.NodeAllocation {
	s := make([]*types.NodeAllocation, 0, len(m))
	for _, v := range m {
		s = append(s, v)
	}
	return s
}
