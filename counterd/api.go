package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
	hclog "github.com/hashicorp/go-hclog"
)

const (
	// NullAttribute is automatically added to an event if no other attributs are provided
	NullAttribute = "null"

	// KeySeperator is used to segment K/V pairs and cannot be used in an attribute key or value
	KeySeperator = ":"
)

const (
	DayInterval = 1 << iota
	WeekInterval
	MonthInterval
)

// RedisClient is used to abstract the client for testing
type RedisClient interface {
	UpdateKeys(keys []string, id string) error
}

// PooledClient uses a connection pool for redis
type PooledClient struct {
	pool *redis.Pool
}

func (p *PooledClient) UpdateKeys(keys []string, id string) error {
	// Get a connection to redis
	c := p.pool.Get()
	defer c.Close()

	// Increment all the keys in a transaction
	c.Send("MULTI")
	for key := range keys {
		c.Send("PFADD", key, id)
	}
	if _, err := c.Do("EXEC"); err != nil {
		return err
	}
	return nil
}

// APIHandler implements the HTTP API endpoints
type APIHandler struct {
	logger hclog.Logger
	client RedisClient
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

	// Generate the keys
	intervals := DateIntervals(DayInterval|WeekInterval|MonthInterval,
		req.Date)
	keys := RequestCounterKeys(intervals, req)

	// Update the keys
	if err := a.client.UpdateKeys(keys, req.ID); err != nil {
		a.logger.Error("failed to update redis", "error", err)
	}
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
	} else {
		for key, value := range r.Attributes {
			if strings.Contains(key, KeySeperator) || strings.Contains(value, KeySeperator) {
				return fmt.Errorf("invalid use of colon in attribute key/value")
			}
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

// RequestCounterKeys returns all the keys that should be incremented for the request
// Key structure is <interval>:<date>:<attr1>:<val1>_<attr2>:...
func RequestCounterKeys(intervals map[string]string, r *IngressRequest) []string {
	// Put the keys into a sorted order
	keys := make([]string, 0, len(r.Attributes))
	for key := range r.Attributes {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	// Build the suffix
	var buf bytes.Buffer
	for idx, key := range keys {
		val := r.Attributes[key]
		if idx != 0 {
			buf.WriteString(KeySeperator)
		}
		buf.WriteString(key)
		buf.WriteString(KeySeperator)
		buf.WriteString(val)
	}
	suffix := buf.String()

	// Construct key per interval
	var out []string
	for interval, date := range intervals {
		var buf bytes.Buffer
		buf.WriteString(interval)
		buf.WriteString(KeySeperator)
		buf.WriteString(date)
		buf.WriteString(KeySeperator)
		buf.WriteString(suffix)
		out = append(out, buf.String())
	}
	return out
}

// DateIntervals returns the formatted intervals for a given
// date and set of interval values
func DateIntervals(intervals int, date time.Time) map[string]string {
	out := make(map[string]string)
	if intervals&DayInterval != 0 {
		out["day"] = date.Format("2006-01-02")
	}
	if intervals&WeekInterval != 0 {
		year, week := date.ISOWeek()
		out["week"] = fmt.Sprintf("%d-%02d", year, week)
	}
	if intervals&MonthInterval != 0 {
		out["month"] = date.Format("2006-01")
	}
	return out
}
