package main

import "time"

// DatabaseClient is used to abstract the DB for testing
type DatabaseClient interface {
	// UpsertDomain is used to register all the domain attributes and values
	UpsertDomain(attributes map[string]map[string]struct{}) error

	// UpsertCounter is used to register the counter value, updating if it exists
	UpsertCounter(interval string, date time.Time, attributes map[string]string, count int64) error
}
