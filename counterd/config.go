package main

import "fmt"
import "github.com/hashicorp/hcl"

// Config is the configuration for the server and snapshot comments
type Config struct {
	// ListenAddress is the HTTP listener address
	ListenAddress string `hcl:"listen_address"`

	// RedisAddress is the address of the redis server
	RedisAddress string `hcl:"redis_address"`

	// PGAddress is the address of the postgresql server
	PGAddress string `hcl:"postgresql_address"`
}

// DefaultConfig returns the default configuration
func DefaultConfig() *Config {
	return &Config{
		ListenAddress: "127.0.0.1:8001",
		RedisAddress:  "127.0.0.1:6379",
		PGAddress:     "127.0.0.1:5432",
	}
}

// ParseConfig is used to parse the configuration
func ParseConfig(raw string) (*Config, error) {
	config := DefaultConfig()

	// Attempt to decode the configuration
	if err := hcl.Decode(config, raw); err != nil {
		return nil, fmt.Errorf("failed to parse config: %v", err)
	}
	return config, nil
}
