package main

import (
	"fmt"
	"strings"
	"time"

	hclog "github.com/hashicorp/go-hclog"
)

// Snapshotter is used to perform snapshotting
type Snapshotter struct {
	config *Config
	logger hclog.Logger
	client RedisClient
}

// Run is used to both snapshot new data and delete old data
func (s *Snapshotter) Run() error {
	// Get the list of keys
	keys, err := s.client.ListKeys()
	if err != nil {
		s.logger.Error("failed to get key list", "error", err)
		return err
	}

	// Parse the keys into a structured form
	parsed, invalid := ParseKeyList(keys)
	if len(invalid) > 0 {
		s.logger.Warn("found invalid keys", "keys", invalid)
	}
	s.logger.Debug(fmt.Sprintf("found %d valid keys", len(parsed)))

	// Determine the filter and delete thresholds
	now := time.Now().UTC()
	updateThreshold := now.Add(-1 * s.config.Snapshot.UpdateThreshold)
	deleteThreshold := now.Add(-1 * s.config.Snapshot.DeleteThreshold)
	s.logger.Info("determining thresholds", "update", updateThreshold,
		"delete", deleteThreshold)

	// Filter the keys
	update, ignore, delete := FilterKeys(parsed, updateThreshold,
		deleteThreshold)
	s.logger.Info("sorting keys", "update", len(update),
		"delete", len(delete), "ignore", len(ignore))

	return nil
}

// FilterKeys sorts the input keys into a set to be updated, deleted, or ignored
func FilterKeys(keys []*ParsedKey, updateThreshold, deleteThreshold time.Time) (update, ignore, delete []*ParsedKey) {
	for _, key := range keys {
		if key.Date.Before(deleteThreshold) {
			delete = append(delete, key)
		} else if key.Date.After(updateThreshold) {
			update = append(update, key)
		} else {
			ignore = append(ignore, key)
		}
	}
	return
}

// ParsedKey represents a raw key
type ParsedKey struct {
	Raw        string
	Interval   string
	Date       time.Time
	Attributes map[string]string
}

// ParseKeyList parses a list of raw keys
func ParseKeyList(keys []string) ([]*ParsedKey, []string) {
	var out []*ParsedKey
	var invalid []string
	for _, key := range keys {
		parsed, err := ParseKey(key)
		if err != nil {
			invalid = append(invalid, key)
		} else {
			out = append(out, parsed)
		}
	}
	return out, invalid
}

// ParseKey parses a single key into a structured form
func ParseKey(raw string) (*ParsedKey, error) {
	// Setup the parsed key
	parsed := &ParsedKey{
		Raw:        raw,
		Attributes: make(map[string]string),
	}

	// Split into the various parts
	parts := strings.Split(raw, KeySeperator)
	if len(parts) < 4 {
		return nil, fmt.Errorf("invalid format")
	}

	// Get the interval
	parsed.Interval = parts[0]

	// Parse the date based on that
	var err error
	switch parsed.Interval {
	case "day":
		parsed.Date, err = time.Parse("2006-01-02", parts[1])
	case "week":
		parsed.Date, err = time.Parse("2006-01-02", parts[1])
	case "month":
		parsed.Date, err = time.Parse("2006-01", parts[1])
	default:
		return nil, fmt.Errorf("invalid interval %q", parsed.Interval)
	}
	if err != nil {
		return nil, fmt.Errorf("invalid date %q", parts[1])
	}

	// Skip past the interval and date
	parts = parts[2:]
	if len(parts)%2 != 0 {
		return nil, fmt.Errorf("key/value attributes not even")
	}

	// Parse all the K/V attributes
	for len(parts) != 0 {
		key := parts[0]
		val := parts[1]
		parsed.Attributes[key] = val
		parts = parts[2:]
	}
	return parsed, nil
}
