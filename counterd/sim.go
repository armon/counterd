package main

import (
	"flag"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/armon/counterd/client"
	hclog "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/uuid"
)

type SimCommand struct{}

func (s *SimCommand) Help() string {
	helpText := `
Usage: counterd sim [flags]

	sim is used to simulate input to the API for testing and benchmarking.

Options:

	-address (Default: "http://127.0.0.1:8001"). Configures the target API address.
	-auth	Provides a bearer token to use.

	-from	Configures the starting range of the date interval. Must be provided with -to.
			Given in RFC3339 format, e.g. 2006-01-02T15:04:05.
	-to		Configures the ending range of the date interval. Must be provided with -from.
			Given in RFC3339 format, e.g. 2006-01-02T15:04:05.
	-num	(Default: 1000). Configures the number of events in the range to generate.

	-a | -attribute key=value	Defines a possible attribute pair. Can be specified multiple times
	to add more keys or values. Events are generated all keys present and a random value.
	`
	return strings.TrimSpace(helpText)
}

func (s *SimCommand) Synopsis() string {
	return "sim generates simulation input to the API"
}

func (s *SimCommand) Run(args []string) int {
	var address, authToken string
	var fromDate, toDate string
	var numEvents int
	attributes := map[string][]string{}
	kvAttr := FlagStringKV(attributes)
	flags := flag.NewFlagSet("counterd", flag.ContinueOnError)
	flags.StringVar(&address, "address", "http://127.0.0.1:8001", "")
	flags.StringVar(&authToken, "auth", "", "")
	flags.StringVar(&fromDate, "from", "", "")
	flags.StringVar(&toDate, "to", "", "")
	flags.IntVar(&numEvents, "num", 1000, "")
	flags.Var(&kvAttr, "attribute", "")
	flags.Var(&kvAttr, "a", "")
	flags.Usage = func() { fmt.Println(s.Help()) }
	if err := flags.Parse(args); err != nil {
		return 1
	}

	// Setup the client
	opts := &client.ClientOptions{
		AuthToken: authToken,
	}
	counterdClient, err := client.NewClient(address, opts)
	if err != nil {
		hclog.Default().Error("Failed to setup client", "error", err)
		return 1
	}

	// Deteremine if this is a fixed range or continuous
	var eventCh <-chan *client.Event
	if fromDate != "" || toDate != "" {
		fromTime, err := time.Parse(time.RFC3339, fromDate)
		if err != nil {
			hclog.Default().Error("Failed to parse from date", "error", err)
			return 1
		}
		toTime, err := time.Parse(time.RFC3339, toDate)
		if err != nil {
			hclog.Default().Error("Failed to parse to date", "error", err)
			return 1
		}
		if fromTime.After(toTime) {
			hclog.Default().Error("From must be before the To time")
			return 1
		}
		if numEvents <= 0 {
			hclog.Default().Error("Must have a non-zero number of events")
			return 1
		}

		eventCh = simulateRange(fromTime, toTime, numEvents, attributes)
	} else {
		eventCh = continuousEvents(attributes)
	}

	// Send all the events
	sent := 0
	for e := range eventCh {
		if err := counterdClient.SendEvent(e); err != nil {
			hclog.Default().Error("Failed to send event", "error", err)
			return 1
		}
		sent++
		if sent%1000 == 0 {
			hclog.Default().Info(fmt.Sprintf("Sent %d events", sent))
		}
	}
	return 0
}

// simulateRange creates a set of events from a given range
func simulateRange(from, to time.Time, numEvents int, attributes map[string][]string) <-chan *client.Event {
	eventCh := make(chan *client.Event, 256)
	go func() {
		defer close(eventCh)
		delta := to.Sub(from) / time.Duration(numEvents)
		current := from

		prefix := uuid.GenerateUUID()[:9]
		for counter := 0; counter < numEvents; counter++ {
			// Create an event
			e := &client.Event{
				ID:         prefix + strconv.Itoa(counter),
				Date:       current,
				Attributes: make(map[string]string),
			}

			// Increment the time
			current = current.Add(delta)

			// Select a random attribute value
			for key, vals := range attributes {
				e.Attributes[key] = vals[rand.Intn(len(vals))]
			}
			eventCh <- e
		}

	}()
	return eventCh
}

// continuousEvents generates events until interrupted
func continuousEvents(attributes map[string][]string) <-chan *client.Event {
	eventCh := make(chan *client.Event, 256)
	go func() {
		prefix := uuid.GenerateUUID()[:9]
		counter := 0
		for {
			// Create an event
			e := &client.Event{
				ID:         prefix + strconv.Itoa(counter),
				Attributes: make(map[string]string),
			}

			// Select a random attribute value
			for key, vals := range attributes {
				e.Attributes[key] = vals[rand.Intn(len(vals))]
			}
			eventCh <- e
			counter++
		}
	}()
	return eventCh
}

// FlagStringKV is a flag.Value implementation for parsing user variables
// from the command-line in the format of '-var key=value', where value is
// only ever a primitive.
type FlagStringKV map[string][]string

func (v *FlagStringKV) String() string {
	return ""
}

func (v *FlagStringKV) Set(raw string) error {
	idx := strings.Index(raw, "=")
	if idx == -1 {
		return fmt.Errorf("No '=' value in arg: %s", raw)
	}

	if *v == nil {
		*v = make(map[string][]string)
	}

	key, value := raw[0:idx], raw[idx+1:]
	(*v)[key] = append((*v)[key], value)
	return nil
}
