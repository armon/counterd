package main

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"
)

// DatabaseClient is used to abstract the DB for testing
type DatabaseClient interface {
	// UpsertDomain is used to register all the domain attributes and values
	UpsertDomain(attributes map[string]map[string]struct{}) error

	// UpsertCounter is used to register the counter value, updating if it exists
	UpsertCounter(interval string, date time.Time, attributes map[string]string, count int64) error
}

// PGDatabase provides a database client backed by PostgreSQL
type PGDatabase struct {
	db *sql.DB

	// Prepared queries we store
	upsertDomain  *sql.Stmt
	upsertCounter *sql.Stmt
}

// NewPGDatabase creates a PGDatabase connection with a URL string
// "postgres://pqgotest:password@localhost/pqgotest?sslmode=verify-full"
func NewPGDatabase(connStr string) (*PGDatabase, error) {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	// Setup the DB connection
	pg := &PGDatabase{
		db: db,
	}

	// Create the prepared queries
	stmt, err := db.Prepare(upsertDomainSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to prepared query: %v", err)
	}
	pg.upsertDomain = stmt

	stmt, err = db.Prepare(upsertCounterSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to prepared query: %v", err)
	}
	pg.upsertCounter = stmt

	return pg, nil
}

const (
	// upsertDomainSQL is used to upsert values into the domain table
	upsertDomainSQL = `INSERT INTO attributes_domain VALUES (?, ?) ON CONFLICT DO NOTHING;`

	// upsertCounterSQL is used to upsert into the counters table
	upsertCounterSQL = `INSERT INTO counters (interval, date, attributes, count) VALUES (?, ?, ?, ?) ON CONFLICT (interval, date, attributes) DO UPDATE SET count = GREATEST(EXCLUDED.count, counters.count);`

	// createDomainSQL is used to create the domain table
	createDomainSQL = `CREATE TABLE IF NOT EXISTS attributes_domain (
		attribute text NOT NULL,
		value text NOT NULL,
		PRIMARY KEY (attribute, value)
	);`

	// createCounterSQL is used to create the counter table
	createCounterSQL = `CREATE TABLE IF NOT EXISTS counters (
	    id uuid DEFAULT uuid_generate_v4(),
		interval varchar(16) NOT NULL,
		date timestamp NOT NULL,
		attributes jsonb NOT NULL,
		count bigint DEFAULT 0,
		PRIMARY KEY (id),
		UNIQUE (interval, date, attributes)
	);`
)
