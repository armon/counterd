package main

import (
	"os"
	"sort"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
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

// IsReidsInteg checks for the INTEG and REDIS_ADDR env vars
func IsRedisInteg() (string, bool) {
	_, ok := os.LookupEnv("INTEG")
	if !ok {
		return "", false
	}
	redisAddr, ok := os.LookupEnv("REDIS_ADDR")
	return redisAddr, ok
}

func TestRedisInteg(t *testing.T) {
	redisAddr, integ := IsRedisInteg()
	if !integ {
		t.SkipNow()
	}

	client, err := NewPooledClient(redisAddr)
	assert.Nil(t, err)

	// Update the keys
	keys := []string{"bar", "baz", "foo"}
	assert.Nil(t, client.UpdateKeys(keys, "1234"))
	assert.Nil(t, client.UpdateKeys(keys, "2345"))

	// Check the keys exist
	out, err := client.ListKeys()
	assert.Nil(t, err)
	assert.Equal(t, keys, out)

	// Verify the counts
	counts, err := client.GetCounts(keys)
	assert.Nil(t, err)
	expect := []int64{2, 2, 2}
	assert.Equal(t, expect, counts)

	// Delete all the keys
	assert.Nil(t, client.DeleteKeys(keys))

	// Ensure there are no keys
	out, err = client.ListKeys()
	assert.Nil(t, err)
	assert.Equal(t, []string{}, out)
}
