package main

import "fmt"
import "github.com/hashicorp/hcl"

// ServerConfig is the configuration for the server
type ServerConfig struct {
	// ListenAddress is the HTTP listener address
	ListenAddress string `hcl:"listen_address"`

	// RedisAddress is the address of the redis server
	RedisAddress string `hcl:"redis_address"`
}

// DefaultConfig returns the default configuration
func DefaultConfig() *ServerConfig {
	return &ServerConfig{
		ListenAddress: "127.0.0.1:8001",
		RedisAddress:  "127.0.0.1:6379",
	}
}

// ParseConfig is used to parse the configuration
func ParseConfig(raw string) (*ServerConfig, error) {
	config := DefaultConfig()

	// Attempt to decode the configuration
	if err := hcl.Decode(config, raw); err != nil {
		return nil, fmt.Errorf("failed to parse config: %v", err)
	}
	return config, nil
}
