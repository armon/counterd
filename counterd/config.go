package main

import (
	"fmt"
	"sort"
	"time"

	"github.com/hashicorp/hcl"
)

const (
	// DefaultUpdateThreshold is the default threshold we update
	// counters if no setting is specified
	DefaultUpdateThreshold = 3 * time.Hour // 3 Hours

	// DefaultDeleteThreshold is the default threshold we delete
	// counters if no setting is specified
	DefaultDeleteThreshold = 3 * 31 * 24 * time.Hour // 31 Days
)

// Config is the configuration for the server and snapshot comments
type Config struct {
	// ListenAddress is the HTTP listener address
	ListenAddress string `hcl:"listen_address"`

	// RedisAddress is the address of the redis server
	RedisAddress string `hcl:"redis_address"`

	// PGAddress is the address of the postgresql server
	PGAddress string `hcl:"postgresql_address"`

	// Snapshot has the snapshot specific configuration
	Snapshot *SnapshotConfig

	// Auth is used to hold authentication configuration
	Auth *AuthConfig

	// Attributes is used to configure filtering of attributes
	Attributes *AttributeConfig
}

// AttributeConfig is used to configure attribute handlign
type AttributeConfig struct {
	// Whitelist is used to restrict the allowed set of attributes
	Whitelist []string

	// Blacklist is used to filter out unwanted attributes
	Blacklist []string
}

// AuthConfig holds the authentication configuration
type AuthConfig struct {
	// Required is used to toggle if we verify authentication
	Required bool `hcl:"required"`

	// Tokens are the allowed bearer tokens via Authorization header
	Tokens []string `hcl:"tokens"`
}

// SnapshotConfig has snapshotting configuration
type SnapshotConfig struct {
	// Cron can be configured to have the server invoke snapshots periodically.
	// This is independent from invoking the snapshot command.
	Cron string `hcl:"cron"`

	// UpdateThreshold is how far back we scan for relevant updates.
	// This prevents old counters from being updated. This should be relative to the
	// snapshot rate. For example, if you snapshot hourly, consider a two hour update threshold.
	UpdateThresholdRaw string        `hcl:"update_threshold"`
	UpdateThreshold    time.Duration `hcl:"-"`

	// DeleteThreshold is how far back a counter needs to be for deletion.
	// This should be at least 2x the longest interval that is tracked. For example,
	// if monthly counters are enabled, consider a two month delete threshold.
	DeleteThresholdRaw string        `hcl:"delete_threshold"`
	DeleteThreshold    time.Duration `hcl:"-"`
}

// DefaultConfig returns the default configuration
func DefaultConfig() *Config {
	return &Config{
		ListenAddress: "127.0.0.1:8001",
		RedisAddress:  "127.0.0.1:6379",
		PGAddress:     "postgres://postgres@localhost/postgres?sslmode=disable",
		Snapshot: &SnapshotConfig{
			UpdateThreshold: DefaultUpdateThreshold,
			DeleteThreshold: DefaultDeleteThreshold,
		},
		Auth: &AuthConfig{
			Required: false,
			Tokens:   []string{},
		},
		Attributes: &AttributeConfig{
			Whitelist: []string{},
			Blacklist: []string{},
		},
	}
}

// ParseConfig is used to parse the configuration
func ParseConfig(raw string) (*Config, error) {
	config := DefaultConfig()

	// Attempt to decode the configuration
	if err := hcl.Decode(config, raw); err != nil {
		return nil, fmt.Errorf("failed to parse config: %v", err)
	}

	if raw := config.Snapshot.UpdateThresholdRaw; raw != "" {
		dur, err := time.ParseDuration(raw)
		if err != nil {
			return nil, fmt.Errorf("failed to parse duration: %v", err)
		}
		config.Snapshot.UpdateThreshold = dur
	}
	if raw := config.Snapshot.DeleteThresholdRaw; raw != "" {
		dur, err := time.ParseDuration(raw)
		if err != nil {
			return nil, fmt.Errorf("failed to parse duration: %v", err)
		}
		config.Snapshot.DeleteThreshold = dur
	}

	// Ensure defaults are provided
	if config.Snapshot.UpdateThreshold == 0 {
		config.Snapshot.UpdateThreshold = DefaultUpdateThreshold
	}
	if config.Snapshot.DeleteThreshold == 0 {
		config.Snapshot.DeleteThreshold = DefaultDeleteThreshold
	}

	// Sort the attribute whitelist and blacklist
	if config.Attributes != nil && config.Attributes.Whitelist != nil {
		sort.Strings(config.Attributes.Whitelist)
	}
	if config.Attributes != nil && config.Attributes.Blacklist != nil {
		sort.Strings(config.Attributes.Blacklist)
	}
	return config, nil
}
