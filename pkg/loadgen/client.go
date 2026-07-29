package loadgen

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type TenantSnapshot struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	MaxWorkers int    `json:"max_workers"`
	Inflight   int    `json:"inflight"`
}

type Task struct {
	TenantID       string `json:"tenant_id"`
	Payload        any    `json:"payload"`
	IdempotencyKey string `json:"idempotency_key"`
}

type ClusterClient interface {
	ListTenants(context.Context) (map[string]TenantSnapshot, error)
	UpsertTenant(context.Context, TenantSpec) error
	SubmitBatch(context.Context, []Task) (accepted int, statusCode int, err error)
}

type HTTPClusterClient struct {
	baseURL string
	client  *http.Client
}

func NewHTTPClusterClient(address string) (*HTTPClusterClient, error) {
	address = strings.TrimRight(strings.TrimSpace(address), "/")
	if !strings.Contains(address, "://") {
		address = "http://" + address
	}
	parsed, err := url.Parse(address)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("invalid controller address %q", address)
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	return &HTTPClusterClient{
		baseURL: parsed.String(),
		client: &http.Client{
			Transport: transport,
			Timeout:   30 * time.Second,
		},
	}, nil
}

func (c *HTTPClusterClient) ListTenants(ctx context.Context) (map[string]TenantSnapshot, error) {
	var tenants map[string]TenantSnapshot
	if _, err := c.doJSON(ctx, http.MethodGet, "/api/v1/admin/tenants", nil, &tenants); err != nil {
		return nil, err
	}
	return tenants, nil
}

func (c *HTTPClusterClient) UpsertTenant(ctx context.Context, spec TenantSpec) error {
	body := map[string]any{"name": spec.Name, "max_workers": spec.MaxWorkers}
	_, err := c.doJSON(
		ctx, http.MethodPut,
		"/api/v1/admin/tenants/"+url.PathEscape(spec.ID), body, nil,
	)
	return err
}

func (c *HTTPClusterClient) SubmitBatch(
	ctx context.Context, tasks []Task,
) (int, int, error) {
	var response struct {
		Tasks []json.RawMessage `json:"tasks"`
	}
	statusCode, err := c.doJSON(
		ctx, http.MethodPost, "/api/v1/tasks/batch",
		map[string]any{"tasks": tasks}, &response,
	)
	if err != nil {
		return 0, statusCode, err
	}
	return len(response.Tasks), statusCode, nil
}

func (c *HTTPClusterClient) doJSON(
	ctx context.Context, method, path string, body, output any,
) (int, error) {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return 0, err
		}
		reader = bytes.NewReader(data)
	}
	request, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return 0, err
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := c.client.Do(request)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(response.Body, 1024))
		return response.StatusCode, fmt.Errorf(
			"%s %s returned %d: %s",
			method, path, response.StatusCode, strings.TrimSpace(string(data)),
		)
	}
	if output != nil {
		if err := json.NewDecoder(response.Body).Decode(output); err != nil {
			return response.StatusCode, err
		}
	}
	return response.StatusCode, nil
}
