package main

import (
	"fmt"
	"io/ioutil"
	"strings"

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

	return 0
}
