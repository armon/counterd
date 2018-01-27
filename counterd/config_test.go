package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestParseConfig_DefaultConfig(t *testing.T) {
	defaultConfig := DefaultConfig()
	config, err := ParseConfig("")
	assert.Nil(t, err)
	assert.Equal(t, defaultConfig.ListenAddress, config.ListenAddress)
	assert.Equal(t, defaultConfig.RedisAddress, config.RedisAddress)
}

func TestParseConfig_Valid(t *testing.T) {
	input := `
listen_address = "127.0.0.1:1234"
redis_address = "127.0.0.1:2345"
postgresql_address = "127.0.0.1:3456"
snapshot {
	update_threshold = "24h"
	delete_threshold = "2000h"
}
	`

	config, err := ParseConfig(input)
	assert.Nil(t, err)
	assert.Equal(t, "127.0.0.1:1234", config.ListenAddress)
	assert.Equal(t, "127.0.0.1:2345", config.RedisAddress)
	assert.Equal(t, "127.0.0.1:3456", config.PGAddress)
	assert.Equal(t, 24*time.Hour, config.Snapshot.UpdateThreshold)
	assert.Equal(t, 2000*time.Hour, config.Snapshot.DeleteThreshold)
}
