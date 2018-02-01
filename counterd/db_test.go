package main

import (
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/assert"
)

type MockCounter struct {
	interval   string
	date       time.Time
	attributes map[string]string
	count      int64
}

func (m *MockCounter) Equal(other *MockCounter) bool {
	return m.interval == other.interval && m.date == other.date && reflect.DeepEqual(m.attributes, other.attributes)
}

type MockDatabaseClient struct {
	domain   map[string]map[string]struct{}
	counters []*MockCounter
	sync.Mutex
}

func NewMockDatabaseClient() *MockDatabaseClient {
	return &MockDatabaseClient{
		domain: make(map[string]map[string]struct{}),
	}
}

func (m *MockDatabaseClient) UpsertDomain(attributes map[string]map[string]struct{}) error {
	m.Lock()
	defer m.Unlock()

	// Merge the new attributes with the existing ones
	for key, values := range attributes {
		existing, ok := m.domain[key]
		if !ok {
			m.domain[key] = values
			continue
		}
		for value := range values {
			existing[value] = struct{}{}
		}
	}
	return nil
}

func (m *MockDatabaseClient) UpsertCounters(counters []*ParsedKey) error {
	m.Lock()
	defer m.Unlock()

OUTER:
	for _, counter := range counters {
		// Create a counter
		c := &MockCounter{
			interval:   counter.Interval,
			date:       counter.Date,
			attributes: counter.Attributes,
			count:      counter.Count,
		}

		// Scan for a matching counter. This is super inefficient but obviously correct.
		for _, existing := range m.counters {
			if existing.Equal(c) {
				// Update the counter, but only monotonically
				if c.count > existing.count {
					existing.count = c.count
				}
				continue OUTER
			}
		}

		// No matching entry
		m.counters = append(m.counters, c)
	}
	return nil
}

// IsDBInteg checks for the INTEG and PG_ADDR env vars
func IsDBInteg() (string, bool) {
	_, ok := os.LookupEnv("INTEG")
	if !ok {
		return "", false
	}
	pgAddr, ok := os.LookupEnv("PG_ADDR")
	return pgAddr, ok
}

func TestPGInit(t *testing.T) {
	pgAddr, integ := IsDBInteg()
	if !integ {
		t.SkipNow()
	}

	db, err := NewPGDatabase(hclog.Default(), pgAddr, false)
	assert.Nil(t, err)
	defer db.DBReset()
	assert.Nil(t, db.DBInit())
}

func TestPGInit_UpsertDomain(t *testing.T) {
	pgAddr, integ := IsDBInteg()
	if !integ {
		t.SkipNow()
	}

	// Setup and then prepare
	db, err := NewPGDatabase(hclog.Default(), pgAddr, false)
	assert.Nil(t, err)
	//defer db.DBReset()
	assert.Nil(t, db.DBInit())
	assert.Nil(t, db.Prepare())

	// Attempt to upsert the domain
	domain := map[string]map[string]struct{}{
		"foo": map[string]struct{}{
			"bar": struct{}{},
			"baz": struct{}{},
		},
		"zip": map[string]struct{}{
			"zap": struct{}{},
		},
	}
	err = db.UpsertDomain(domain)
	assert.Nil(t, err)

	// Test redundant insert
	err = db.UpsertDomain(domain)
	assert.Nil(t, err)
}

func TestPGInit_UpsertCounters(t *testing.T) {
	pgAddr, integ := IsDBInteg()
	if !integ {
		t.SkipNow()
	}

	// Setup and then prepare
	db, err := NewPGDatabase(hclog.Default(), pgAddr, false)
	assert.Nil(t, err)
	//defer db.DBReset()
	assert.Nil(t, db.DBInit())
	assert.Nil(t, db.Prepare())

	// Setup some fake counters
	p1, _ := ParseKey("day:2017-01-18:foo:bar")
	p1.Count = 10
	p2, _ := ParseKey("day:2017-01-10:foo:baz")
	p2.Count = 20
	p3, _ := ParseKey("day:2017-01-01:zip:zap")
	p3.Count = 30
	counters := []*ParsedKey{p1, p2, p3}

	// Attempt to upsert the counters
	err = db.UpsertCounters(counters)
	assert.Nil(t, err)

	// Test redundant insert
	err = db.UpsertCounters(counters)
	assert.Nil(t, err)
}
