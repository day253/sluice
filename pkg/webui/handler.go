// Package webui serves the embedded Sluice dashboard at /.
package webui

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed index.html
var content embed.FS

// Handler returns an http.Handler serving the dashboard on / and falling
// back to the given API handler for /api/* routes.
func Handler(apiHandler http.Handler) http.Handler {
	uiFile, err := fs.ReadFile(content, "index.html")
	if err != nil {
		panic("webui: embedded index.html not found")
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Route /api/* to the API handler.
		if len(r.URL.Path) >= 4 && r.URL.Path[:4] == "/api" {
			apiHandler.ServeHTTP(w, r)
			return
		}
		// Everything else → dashboard.
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(uiFile)
	})
}
