package main

import (
	"reflect"
	"sync"
	"time"
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

func (m *MockDatabaseClient) UpsertCounter(interval string, date time.Time, attributes map[string]string, count int64) error {
	m.Lock()
	defer m.Unlock()

	// Create a counter
	c := &MockCounter{
		interval:   interval,
		date:       date,
		attributes: attributes,
		count:      count,
	}

	// Scan for a matching counter. This is super inefficient but obviously correct.
	for _, existing := range m.counters {
		if existing.Equal(c) {
			// Update the counter, but only monotonically
			if c.count > existing.count {
				existing.count = c.count
			}
			return nil
		}
	}

	// No matching entry
	m.counters = append(m.counters, c)
	return nil
}
