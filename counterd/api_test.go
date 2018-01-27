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

func TestRequestCounterKeys(t *testing.T) {
	intervals := map[string]string{
		"day":   "2018-01-27",
		"month": "2018-01",
	}
	r := &IngressRequest{
		ID: "1234",
		Attributes: map[string]string{
			"foo": "bar",
			"baz": "zip",
		},
	}

	keys := RequestCounterKeys(intervals, r)
	assert.Equal(t, 2, len(keys))

	dayKey := "day:2018-01-27:baz:zip:foo:bar"
	assert.Contains(t, keys, dayKey)

	monthKey := "month:2018-01:baz:zip:foo:bar"
	assert.Contains(t, keys, monthKey)
}

func TestDateIntervals(t *testing.T) {
	intervals := DayInterval | WeekInterval | MonthInterval
	date, err := time.Parse(time.RFC3339, "2006-01-09T15:04:05Z")
	assert.Nil(t, err)
	out := DateIntervals(intervals, date)

	assert.Equal(t, 3, len(out))

	dayFormat := "2006-01-09"
	assert.Equal(t, dayFormat, out["day"])

	weekFormat := "2006-02"
	assert.Equal(t, weekFormat, out["week"])

	monthFormat := "2006-01"
	assert.Equal(t, monthFormat, out["month"])
}
