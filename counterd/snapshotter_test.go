package main

import (
	"testing"
	"time"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/assert"
)

func TestSnapshotter(t *testing.T) {
	conf := DefaultConfig()
	conf.Snapshot.DeleteThreshold = 14 * 24 * time.Hour
	redis := NewMockRedisClient()
	db := NewMockDatabaseClient()

	snap := &Snapshotter{
		config: conf,
		logger: hclog.Default(),
		client: redis,
		db:     db,
	}

	// Create some counter values
	keys := []string{
		"day:2017-01-18:foo:bar",
		"day:2017-01-10:foo:baz",
		"day:2017-01-01:zip:zap",
	}
	assert.Nil(t, redis.UpdateKeys(keys, "1234"))

	// Run the snapshot
	runTime := time.Date(2017, 1, 18, 12, 0, 0, 0, time.UTC)
	err := snap.Run(runTime)
	assert.Nil(t, err)

	// Check that the oldest key is deleted
	counters, _ := redis.ListKeys()
	assert.Equal(t, 2, len(counters))
	assert.NotContains(t, counters, "day:2017-01-01:zip:zap")

	// Check that the newest counter is updated
	assert.Equal(t, 1, len(db.counters))

	// Check that the domain is updated
	domain := map[string]map[string]struct{}{
		"foo": map[string]struct{}{
			"bar": struct{}{},
		},
	}
	assert.Equal(t, domain, db.domain)
}

func TestCollectDomain(t *testing.T) {
	p1, _ := ParseKey("day:2017-01-18:foo:bar")
	p2, _ := ParseKey("day:2017-01-10:foo:baz")
	p3, _ := ParseKey("day:2017-01-01:zip:zap")

	inp := []*ParsedKey{p1, p2, p3}
	attributes := CollectDomain(inp)

	assert.Contains(t, attributes, "foo")
	assert.Contains(t, attributes["foo"], "bar")
	assert.Contains(t, attributes["foo"], "baz")

	assert.Contains(t, attributes, "zip")
	assert.Contains(t, attributes["zip"], "zap")
}

func TestFilterKeys(t *testing.T) {
	p1, _ := ParseKey("day:2017-01-18:foo:bar")
	p2, _ := ParseKey("day:2017-01-10:foo:bar")
	p3, _ := ParseKey("day:2017-01-01:foo:bar")

	inp := []*ParsedKey{p1, p2, p3}
	updateThres := time.Date(2017, 1, 17, 0, 0, 0, 0, time.UTC)
	deleteThres := time.Date(2017, 1, 9, 0, 0, 0, 0, time.UTC)
	update, ignore, delete := FilterKeys(inp, updateThres, deleteThres)

	assert.Contains(t, update, p1)
	assert.Contains(t, ignore, p2)
	assert.Contains(t, delete, p3)
}

func TestParseKeyList(t *testing.T) {
	input := []string{
		"day:2017-01-18:foo:bar",
		"week:2017-12-18:foo:bar:zip:zap",
		"month",
	}
	out, invalid := ParseKeyList(input)
	assert.Equal(t, 2, len(out))
	assert.Equal(t, 1, len(invalid))
}

func TestParseKey(t *testing.T) {
	type tcase struct {
		Input    string
		Expected *ParsedKey
		Err      string
	}
	tcases := []tcase{
		{
			Input: "day:2017-01-18:foo:bar",
			Expected: &ParsedKey{
				Interval: "day",
				Date:     time.Date(2017, 1, 18, 0, 0, 0, 0, time.UTC),
				Attributes: map[string]string{
					"foo": "bar",
				},
			},
		},
		{
			Input: "week:2017-12-18:foo:bar:zip:zap",
			Expected: &ParsedKey{
				Interval: "week",
				Date:     time.Date(2017, 12, 18, 0, 0, 0, 0, time.UTC),
				Attributes: map[string]string{
					"foo": "bar",
					"zip": "zap",
				},
			},
		},
		{
			Input: "month:2017-12:foo:bar:zip:zap",
			Expected: &ParsedKey{
				Interval: "month",
				Date:     time.Date(2017, 12, 1, 0, 0, 0, 0, time.UTC),
				Attributes: map[string]string{
					"foo": "bar",
					"zip": "zap",
				},
			},
		},
		{
			Input: "month:2017:foo:bar:zip:zap",
			Err:   "invalid date \"2017\"",
		},
		{
			Input: "foo:2017:foo:bar:zip:zap",
			Err:   "invalid interval \"foo\"",
		},
		{
			Input: "month:2017-12:foo:bar:zip",
			Err:   "key/value attributes not even",
		},
		{
			Input: "month:zip",
			Err:   "invalid format",
		},
	}

	for _, tc := range tcases {
		out, err := ParseKey(tc.Input)
		if tc.Err == "" {
			assert.Nil(t, err)
			tc.Expected.Raw = tc.Input
			assert.Equal(t, tc.Expected, out)
		} else {
			assert.Equal(t, tc.Err, err.Error())
		}
	}
}
