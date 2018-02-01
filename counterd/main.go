package main

import (
	"log"
	"os"

	"github.com/mitchellh/cli"
)

func main() {
	c := cli.NewCLI("counterd", "0.1.0")
	c.Args = os.Args[1:]
	c.Commands = map[string]cli.CommandFactory{
		"dbinit": func() (cli.Command, error) {
			return &DBInitCommand{}, nil
		},
		"server": func() (cli.Command, error) {
			return &ServerCommand{}, nil
		},
		"snapshot": func() (cli.Command, error) {
			return &SnapshotCommand{}, nil
		},
	}

	exitStatus, err := c.Run()
	if err != nil {
		log.Println(err)
	}
	os.Exit(exitStatus)
}
