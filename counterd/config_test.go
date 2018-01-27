package main

import (
	"testing"

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
	`

	config, err := ParseConfig(input)
	assert.Nil(t, err)
	assert.Equal(t, "127.0.0.1:1234", config.ListenAddress)
	assert.Equal(t, "127.0.0.1:2345", config.RedisAddress)
}
