package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Client provides a high level API client for counterd
type Client struct {
	addr string
	opts *ClientOptions
}

// ClientOptions is used to configure the client
type ClientOptions struct {
	// AuthToken is used to send a Bearer token with requests for authorization
	AuthToken string
}

// NewClient returns a new client for the given address and options
func NewClient(addr string, opts *ClientOptions) (*Client, error) {
	c := &Client{
		addr: addr,
		opts: opts,
	}
	return c, nil
}

// SendEvent is used to submit an event to be ingressed
func (c *Client) SendEvent(e *Event) error {
	// Marshal the event
	raw, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %v", err)
	}

	// Setup the request
	req, err := http.NewRequest("PUT", c.addr+"/v1/ingress", bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("failed to setup request: %v", err)
	}

	// Check if we should add an Auth header
	if c.opts != nil && c.opts.AuthToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.opts.AuthToken)
	}

	// Send the request
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to make request: %v", err)
	}

	// Verify we got a 200 OK
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bad response code %d", resp.StatusCode)
	}
	return nil
}

// Event is used to provide a structured input
type Event struct {
	// Unique identifier for this event
	ID string `json:"id"`

	// Date of the event, set to the current time if omitted.
	// Expected to be RFC 3339 format.
	Date time.Time `json:",omitempty"`

	// Attributes are an opaque set of key/value pairs. If none provided, the
	// special NullAttribute will be automatically injected.
	Attributes map[string]string `json:"attributes,omitempty"`
}
