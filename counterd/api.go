package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	hclog "github.com/hashicorp/go-hclog"
)

const (
	// NullAttribute is automatically added to an event if no other attributs are provided
	NullAttribute = "null"
)

// APIHandler implements the HTTP API endpoints
type APIHandler struct {
	logger hclog.Logger
}

func (a *APIHandler) Ingress(w http.ResponseWriter, r *http.Request) {
	// Verify the method
	if r.Method != "PUT" {
		w.WriteHeader(405)
		return
	}

	// Parse the request body
	req, err := ParseIngressRequest(r.Body)
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte(fmt.Sprintf("Invalid Request: %s", err)))
		return
	}
	a.logger.Debug("Ingress event", "id", req.ID, "attributes", req.Attributes)
}

func (a *APIHandler) Query(w http.ResponseWriter, r *http.Request) {
	// Verify the method
	if r.Method != "GET" {
		w.WriteHeader(405)
		return
	}
}
func (a *APIHandler) Domain(w http.ResponseWriter, r *http.Request) {
	// Verify the method
	if r.Method != "GET" {
		w.WriteHeader(405)
		return
	}
}

func (a *APIHandler) Range(w http.ResponseWriter, r *http.Request) {
	// Verify the method
	if r.Method != "GET" {
		w.WriteHeader(405)
		return
	}
}

// IngressRequest is input for ingress as a JSON object
type IngressRequest struct {
	// Unique identifier for this event
	ID string

	// Date of the event, set to the current time if omitted.
	// Expected to be RFC 3339 format.
	Date time.Time

	// Attributes are an opaque set of key/value pairs. If none provided, the
	// special NullAttribute will be automatically injected.
	Attributes map[string]string
}

// Validate is used to sanity check a request and initialize defaults
func (r *IngressRequest) Validate() error {
	// Ensure there is an ID
	if r.ID == "" {
		return fmt.Errorf("missing request ID")
	}

	// Fill in the date if missing
	if r.Date.IsZero() {
		r.Date = time.Now()
	}

	// Inject the null attribute if necessary
	if len(r.Attributes) == 0 {
		r.Attributes = map[string]string{
			NullAttribute: NullAttribute,
		}
	}
	return nil
}

// ParseIngress is used to parse an ingress request from a reader
func ParseIngressRequest(r io.Reader) (*IngressRequest, error) {
	var req IngressRequest

	// Attempt to parse the request
	if err := json.NewDecoder(r).Decode(&req); err != nil {
		return nil, fmt.Errorf("failed to parse: %v", err)
	}

	// Validate the request
	if err := req.Validate(); err != nil {
		return nil, err
	}

	// Return the request
	return &req, nil
}
