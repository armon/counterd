package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	hclog "github.com/hashicorp/go-hclog"
	lru "github.com/hashicorp/golang-lru"
	_ "github.com/lib/pq"
)

const (
	// TransactionSizeLimit is the limit of operations per single transaction
	TransactionSizeLimit = 256

	// AttributeCacheSize is used to cache the attributes to avoid useless transactions
	AttributeCacheSize = 32 * 1024
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
	logger hclog.Logger
	db     *sql.DB

	// Prepared queries we store
	upsertDomain  *sql.Stmt
	upsertCounter *sql.Stmt

	attrCache *lru.TwoQueueCache
}

// NewPGDatabase creates a PGDatabase connection with a URL string
// "postgres://pqgotest:password@localhost/pqgotest?sslmode=verify-full"
func NewPGDatabase(logger hclog.Logger, connStr string) (*PGDatabase, error) {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	// Create a new attribute cache
	cache, _ := lru.New2Q(AttributeCacheSize)

	// Setup the DB connection
	pg := &PGDatabase{
		logger:    logger,
		db:        db,
		attrCache: cache,
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

// DBInit is used to initialize the database and create tables/indexes
func (p *PGDatabase) DBInit() error {
	// Get a connection
	ctx := context.Background()
	conn, err := p.db.Conn(ctx)
	if err != nil {
		p.logger.Error("failed to get database connection", "error", err)
		return err
	}
	defer conn.Close()

	// Create the tables
	if _, err := conn.ExecContext(ctx, createDomainSQL); err != nil {
		p.logger.Error("failed to create domain table", "error", err)
		return err
	}
	if _, err := conn.ExecContext(ctx, createCounterSQL); err != nil {
		p.logger.Error("failed to create counter table", "error", err)
		return err
	}
	return nil
}

func (p *PGDatabase) UpsertDomain(attributes map[string]map[string]struct{}) error {
	// Flatten all the input pairs
	type tuple struct {
		key, value string
	}
	var tuples []tuple
	for attr, values := range attributes {
		for val := range values {
			tuple := tuple{attr, val}
			if !p.attrCache.Contains(tuple) {
				tuples = append(tuples, tuple)
			}
		}
	}

	// Get a connection
	ctx := context.Background()
	conn, err := p.db.Conn(ctx)
	if err != nil {
		p.logger.Error("failed to get database connection", "error", err)
		return err
	}
	defer conn.Close()

	// Handle the inputs in chunks to limit transaction size
	for n := len(tuples); n > 0; {
		var chunk []tuple
		if n > TransactionSizeLimit {
			chunk = tuples[:TransactionSizeLimit]
			tuples = tuples[TransactionSizeLimit:]
		} else {
			chunk = tuples
			tuples = nil
		}

		// Create a transaction
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			p.logger.Error("failed to start transaction", "error", err)
			return err
		}

		// Do all the updates in the transaction
		for _, tuple := range chunk {
			if _, err := tx.Exec(upsertDomainSQL, tuple.key, tuple.value); err != nil {
				p.logger.Error("failed to update domain table", "key", tuple.key,
					"value", tuple.value, "error", err)
				return err
			}
		}

		// Commit all the updates
		if tx.Commit(); err != nil {
			p.logger.Error("failed to commit transaction", "error", err)
			return err
		}

		// Add to the cache
		for _, tuple := range chunk {
			p.attrCache.Add(tuple, struct{}{})
		}
	}
	return nil
}

func (p *PGDatabase) UpsertCounter(interval string, date time.Time, attributes map[string]string, count int64) error {
	return nil
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
