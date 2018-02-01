package main

import (
	"fmt"
	"io/ioutil"
	"strings"

	hclog "github.com/hashicorp/go-hclog"
)

type DBInitCommand struct{}

func (s *DBInitCommand) Help() string {
	helpText := `
Usage: counterd dbinit <config>

	dbinit is used to initialize the database and create the appropriate
	tables and indexes. The path to the configuration file must be provided.
	`
	return strings.TrimSpace(helpText)
}

func (s *DBInitCommand) Synopsis() string {
	return "dbinit initializes the database"
}

func (s *DBInitCommand) Run(args []string) int {
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

	// Attempt to connect to the database
	pg, err := NewPGDatabase(hclog.Default().Named("postgresql"), config.PGAddress, false)
	if err != nil {
		hclog.Default().Error("Failed to setup database connection", "error", err)
		return 1
	}

	// Attempt to initialize
	if err := pg.DBInit(); err != nil {
		hclog.Default().Error("Failed to initialize database", "error", err)
		return 1
	}
	hclog.Default().Info("Database initialized")
	return 0
}
