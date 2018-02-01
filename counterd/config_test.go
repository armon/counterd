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
	cron = "@hourly"
	update_threshold = "24h"
	delete_threshold = "2000h"
}
auth {
	required = true
	tokens = ["1234", "2345"]
}
attributes {
	whitelist = ["name", "color"]
	blacklist = ["src", "ip"]
}
	`

	config, err := ParseConfig(input)
	assert.Nil(t, err)
	assert.Equal(t, "127.0.0.1:1234", config.ListenAddress)
	assert.Equal(t, "127.0.0.1:2345", config.RedisAddress)
	assert.Equal(t, "127.0.0.1:3456", config.PGAddress)

	assert.Equal(t, 24*time.Hour, config.Snapshot.UpdateThreshold)
	assert.Equal(t, 2000*time.Hour, config.Snapshot.DeleteThreshold)
	assert.Equal(t, "@hourly", config.Snapshot.Cron)

	assert.Equal(t, true, config.Auth.Required)
	tokens := []string{"1234", "2345"}
	assert.Equal(t, tokens, config.Auth.Tokens)

	// Expect the lists to be sorted
	white := []string{"color", "name"}
	assert.Equal(t, white, config.Attributes.Whitelist)
	black := []string{"ip", "src"}
	assert.Equal(t, black, config.Attributes.Blacklist)
}
