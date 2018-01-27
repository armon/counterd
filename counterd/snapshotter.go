package main

import hclog "github.com/hashicorp/go-hclog"

// Snapshotter is used to perform snapshotting
type Snapshotter struct {
	config *Config
	logger hclog.Logger
	client RedisClient
}

// Run is used to both snapshot new data and delete old data
func (s *Snapshotter) Run() error {
	return nil
}
