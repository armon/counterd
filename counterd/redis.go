package main

import (
	"sort"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
)

const (
	// RedisKeyPrefix is prefixed to all keys for namespacing
	RedisKeyPrefix = "counterd:"

	// ScanCount is the number of entries scanned at a time
	ScanCount = 100
)

// RedisClient is used to abstract the client for testing
type RedisClient interface {
	// UpdateKeys sets the ID for each of the given keys
	UpdateKeys(keys []string, id string) error

	// ListKeys returns all the keys in sorted order
	ListKeys() ([]string, error)

	// GetCounts returns the counts for the given keys
	GetCounts(keys []string) ([]int64, error)

	// DeleteKeys deletes a set of keys
	DeleteKeys([]string) error
}

// PooledClient uses a connection pool for redis
type PooledClient struct {
	pool *redis.Pool
}

// Setup the redis pool
func NewPooledClient(addr string) (*PooledClient, error) {
	pool := &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 30 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.DialURL(addr) },
	}
	pc := &PooledClient{
		pool: pool,
	}
	return pc, nil
}

func (p *PooledClient) UpdateKeys(keys []string, id string) error {
	// Fast path on no-op
	if len(keys) == 0 {
		return nil
	}

	// Get a connection to redis
	c := p.pool.Get()
	defer c.Close()

	// Increment all the keys in a transaction
	c.Send("MULTI")
	for _, key := range keys {
		c.Send("PFADD", RedisKeyPrefix+key, id)
	}
	if _, err := c.Do("EXEC"); err != nil {
		return err
	}
	return nil
}

func (p *PooledClient) ListKeys() ([]string, error) {
	// Get a connection to redis
	c := p.pool.Get()
	defer c.Close()

	// Track all the keys in a map, since redis may return duplicates
	keyMap := make(map[string]struct{})
	var cursor int64 = 0
	for {
		respSet, err := redis.Values(c.Do("SCAN", cursor, "MATCH", RedisKeyPrefix+"*", "COUNT", ScanCount))
		if err != nil {
			return nil, err
		}

		// Scan all the keys
		keys := respSet[1].([]interface{})
		for _, keyRaw := range keys {
			keyMap[string(keyRaw.([]byte))] = struct{}{}
		}

		// Update the cursor
		cursor, err = redis.Int64(respSet[0], nil)
		if err != nil {
			return nil, err
		}
		if cursor == 0 {
			break
		}
	}

	// Convert the map to a flat list
	keys := make([]string, 0, len(keyMap))
	for key := range keyMap {
		keys = append(keys, strings.TrimPrefix(key, RedisKeyPrefix))
	}
	sort.Strings(keys)
	return keys, nil
}

func (p *PooledClient) GetCounts(keys []string) ([]int64, error) {
	// Fast path on no-op
	if len(keys) == 0 {
		return nil, nil
	}

	// Get a connection to redis
	c := p.pool.Get()
	defer c.Close()

	// Count all the keys in a transaction
	c.Send("MULTI")
	for _, key := range keys {
		c.Send("PFCOUNT", RedisKeyPrefix+key)
	}
	raw, err := c.Do("EXEC")
	if err != nil {
		return nil, err
	}
	rawList := raw.([]interface{})

	// Parse the result
	out := make([]int64, len(keys))
	for idx := range keys {
		count := rawList[idx].(int64)
		out[idx] = count
	}
	return out, nil
}

func (p *PooledClient) DeleteKeys(keys []string) error {
	// Fast path on no-op
	if len(keys) == 0 {
		return nil
	}

	// Get a connection to redis
	c := p.pool.Get()
	defer c.Close()

	// Convert from string list to interface list
	intList := make([]interface{}, len(keys))
	for idx, key := range keys {
		intList[idx] = RedisKeyPrefix + key
	}

	// Delete all the keys
	if _, err := c.Do("DEL", intList...); err != nil {
		return err
	}
	return nil
}
