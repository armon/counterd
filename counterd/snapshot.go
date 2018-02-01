package main

import (
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	hclog "github.com/hashicorp/go-hclog"
)

type SnapshotCommand struct{}

func (s *SnapshotCommand) Help() string {
	helpText := `
Usage: counterd snapshot <config>

	Snapshot is used to snapshot data from redis and update the database.
	The path to the configuration file must be provided.

	`
	return strings.TrimSpace(helpText)
}

func (s *SnapshotCommand) Synopsis() string {
	return "Snapshot data from redis into the database"
}

func (s *SnapshotCommand) Run(args []string) int {
	// Check that we got exactly one argument
	if l := len(args); l != 1 {
		fmt.Println(s.Help())
		return 1
	}

	// Attempt to parse the config
	filename := args[0]
	raw, err := ioutil.ReadFile(filename)
	if err != nil {
		hclog.Default().Error("Failed to load configuration file", "file", filename, "error", err)
		return 1
	}

	// Parse the config
	config, err := ParseConfig(string(raw))
	if err != nil {
		hclog.Default().Error("Failed to parse configuration file", "error", err)
		return 1
	}
	hclog.Default().Info("Connecting to postgresql", "addr", config.PGAddress)

	// Setup the redis pool
	client, err := NewPooledClient(config.RedisAddress)
	if err != nil {
		hclog.Default().Error("Failed to setup redis connection", "error", err)
		return 1
	}

	// Create the snapshotter
	snap := &Snapshotter{
		config: config,
		logger: hclog.Default().Named("snapshotter"),
		client: client,
	}

	// Run the snapshotter now
	if err := snap.Run(time.Now().UTC()); err != nil {
		hclog.Default().Error("Failed to snapshot", "error", err)
		return 1
	}
	return 0
}
