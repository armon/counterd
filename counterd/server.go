package main

import (
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strings"

	hclog "github.com/hashicorp/go-hclog"
)

type ServerCommand struct{}

func (s *ServerCommand) Help() string {
	helpText := `
Usage: counterd server <config>

	Server is used to run the main process serving the API.
	The path to the configuration file must be provided.

	`
	return strings.TrimSpace(helpText)
}

func (s *ServerCommand) Synopsis() string {
	return "Runs the main process serving the API"
}

func (s *ServerCommand) Run(args []string) int {
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

	// Start a TCP listener
	ln, err := net.Listen("tcp", config.ListenAddress)
	if err != nil {
		hclog.Default().Error("Failed to start listener", "error", err)
		return 1
	}
	hclog.Default().Info("Listener started", "address", config.ListenAddress)

	// Setup the redis pool
	hclog.Default().Info("Connecting to redis", "addr", config.RedisAddress)
	client, err := NewPooledClient(config.RedisAddress)
	if err != nil {
		hclog.Default().Error("Failed to setup redis connection", "error", err)
		return 1
	}

	// Attempt to connect to the database
	hclog.Default().Info("Connecting to postgresql", "addr", config.PGAddress)
	pg, err := NewPGDatabase(hclog.Default().Named("postgresql"), config.PGAddress, true)
	if err != nil {
		hclog.Default().Error("Failed to setup database connection", "error", err)
		return 1
	}

	// Setup the endpoint handlers
	api := &APIHandler{
		logger: hclog.Default().Named("api"),
		client: client,
		db:     pg,
	}

	// Setup the HTTP handler
	mux := NewHTTPHandler(api)

	// Start the HTTP server
	if err := http.Serve(ln, mux); err != nil {
		hclog.Default().Error("HTTP listener failed", "error", err)
	}
	return 0
}

// NewHTTPHandler creates a new router to all the endpoints
func NewHTTPHandler(api *APIHandler) http.Handler {
	// Create a muxer with all the routes
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/ingress", api.Ingress)
	mux.HandleFunc("/v1/query/", api.Query)
	mux.HandleFunc("/v1/domain/", api.Domain)
	mux.HandleFunc("/v1/range/", api.Range)
	mux.HandleFunc("/ui", nil)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/ui", 301)
	})
	return mux
}
