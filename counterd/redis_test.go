package main

import (
	"sort"
	"sync"
)

type MockRedisClient struct {
	counters map[string]map[string]struct{}
	sync.Mutex
}

func NewMockRedisClient() *MockRedisClient {
	return &MockRedisClient{
		counters: make(map[string]map[string]struct{}),
	}
}

func (m *MockRedisClient) UpdateKeys(keys []string, id string) error {
	m.Lock()
	defer m.Unlock()
	for _, key := range keys {
		vals := m.counters[key]
		if vals == nil {
			vals = make(map[string]struct{})
			m.counters[key] = vals
		}
		vals[id] = struct{}{}
	}
	return nil
}

func (m *MockRedisClient) ListKeys() ([]string, error) {
	m.Lock()
	defer m.Unlock()

	out := make([]string, 0, len(m.counters))
	for key := range m.counters {
		out = append(out, key)
	}
	sort.Strings(out)
	return out, nil
}

func (m *MockRedisClient) GetCounts(keys []string) ([]int64, error) {
	m.Lock()
	defer m.Unlock()

	out := make([]int64, len(keys))
	for idx, key := range keys {
		ids := m.counters[key]
		out[idx] = int64(len(ids))
	}
	return out, nil
}

func (m *MockRedisClient) DeleteKeys(keys []string) error {
	m.Lock()
	defer m.Unlock()
	for _, key := range keys {
		delete(m.counters, key)
	}
	return nil
}
