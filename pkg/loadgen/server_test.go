package loadgen

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHandlerAcceptsOnlyCompactParametersAndExposesCurrentState(t *testing.T) {
	manager := NewManager(newFakeClusterClient(), ManagerConfig{
		PollInterval:      time.Millisecond,
		DrainDeadline:     time.Second,
		ZeroConfirmations: 1,
	})
	defer manager.Close()
	handler := NewHandler(manager)

	request := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/load-runs",
		strings.NewReader(`{
			"name":"compact","recipe":"unit","operation":"load",
			"options":{"tenantCount":2,"tasksPerTenant":3,"quota":4}
		}`),
	)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("POST status = %d body=%s", response.Code, response.Body.String())
	}
	var started struct {
		Run Run `json:"run"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &started); err != nil {
		t.Fatal(err)
	}
	run, err := manager.Wait(started.Run.ID, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "completed" || run.TotalTasks != 6 || run.Submitted != 6 {
		t.Fatalf("completed run = %+v", run)
	}

	response = httptest.NewRecorder()
	handler.ServeHTTP(
		response,
		httptest.NewRequest(http.MethodGet, "/api/v1/load-runs/current", nil),
	)
	if response.Code != http.StatusOK ||
		!strings.Contains(response.Body.String(), `"status":"completed"`) {
		t.Fatalf("GET current status=%d body=%s", response.Code, response.Body.String())
	}

	response = httptest.NewRecorder()
	handler.ServeHTTP(
		response,
		httptest.NewRequest(
			http.MethodPost,
			"/api/v1/load-runs",
			bytes.NewBufferString(`{"operation":"load","tasks":[{"payload":"expanded"}]}`),
		),
	)
	if response.Code != http.StatusBadRequest ||
		!strings.Contains(response.Body.String(), "unknown field") {
		t.Fatalf("expanded task document status=%d body=%s", response.Code, response.Body.String())
	}
}
