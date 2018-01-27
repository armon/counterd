package main

import "net/http"

// NewHTTPHandler creates a new router to all the endpoints
func NewHTTPHandler(api *APIHandler) http.Handler {
	// Create a muxer with all the routes
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/ingress", api.Ingress)
	mux.HandleFunc("/v1/query/", api.Query)
	mux.HandleFunc("/v1/domain/", api.Domain)
	mux.HandleFunc("/v1/range/", api.Range)
	mux.HandleFunc("/ui", nil)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/ui", 301)
	})
	return mux
}
