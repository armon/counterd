package main

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestIngressRequest_Validate(t *testing.T) {
	// Create a blank request
	r := &IngressRequest{}
	assert.NotNil(t, r.Validate())

	// Set an ID, should be fine
	r.ID = "12345"
	assert.Nil(t, r.Validate())

	// Check that date is initialized
	assert.WithinDuration(t, time.Now(), r.Date, time.Second)

	// Check that we have the null attribute
	assert.Contains(t, r.Attributes, NullAttribute)
}

func TestIngressRequest_Parse(t *testing.T) {
	input := `{"id": "1234", "date": "2009-11-10T23:00:00Z", "attributes": {"foo": "bar"}}`
	req, err := ParseIngressRequest(strings.NewReader(input))
	assert.Nil(t, err)
	assert.Equal(t, "1234", req.ID)

	date, err := time.Parse(time.RFC3339, "2009-11-10T23:00:00Z")
	assert.Nil(t, err)
	assert.Equal(t, date, req.Date)

	assert.Contains(t, req.Attributes, "foo")
	assert.Equal(t, "bar", req.Attributes["foo"])
}
