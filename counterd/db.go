package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	hclog "github.com/hashicorp/go-hclog"
	lru "github.com/hashicorp/golang-lru"
	_ "github.com/lib/pq"
)

const (
	// TransactionSizeLimit is the limit of operations per single transaction
	TransactionSizeLimit = 256

	// AttributeCacheSize is used to cache the attributes to avoid updates
	AttributeCacheSize = 32 * 1024

	// CounterCacheSize is used to cache counter values to avoid updates
	CounterCacheSize = 32 * 1024
)

// DatabaseClient is used to abstract the DB for testing
type DatabaseClient interface {
	// UpsertDomain is used to register all the domain attributes and values
	UpsertDomain(attributes map[string]map[string]struct{}) error

	// UpsertCounters is used to register the counter value, updating if it exists
	UpsertCounters(updates []*ParsedKey) error
}

// PGDatabase provides a database client backed by PostgreSQL
type PGDatabase struct {
	logger hclog.Logger
	db     *sql.DB

	// Prepared queries we store
	upsertDomain  *sql.Stmt
	upsertCounter *sql.Stmt

	attrCache    *lru.TwoQueueCache
	counterCache *lru.TwoQueueCache
}

// NewPGDatabase creates a PGDatabase connection with a URL string
// "postgres://pqgotest:password@localhost/pqgotest?sslmode=verify-full"
func NewPGDatabase(logger hclog.Logger, connStr string, prepare bool) (*PGDatabase, error) {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	// Create a new attribute cache
	attrCache, _ := lru.New2Q(AttributeCacheSize)
	counterCache, _ := lru.New2Q(CounterCacheSize)

	// Setup the DB connection
	pg := &PGDatabase{
		logger:       logger,
		db:           db,
		attrCache:    attrCache,
		counterCache: counterCache,
	}

	// Create the prepared queries
	if prepare {
		if err := pg.Prepare(); err != nil {
			return nil, err
		}
	}
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
	if _, err := conn.ExecContext(ctx, createExtension); err != nil {
		p.logger.Error("failed to create UUID extension", "error", err)
		return err
	}
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

// DBReset is used to drop the tables/indexes
func (p *PGDatabase) DBReset() error {
	// Get a connection
	ctx := context.Background()
	conn, err := p.db.Conn(ctx)
	if err != nil {
		p.logger.Error("failed to get database connection", "error", err)
		return err
	}
	defer conn.Close()

	// Drop the tables
	if _, err := conn.ExecContext(ctx, dropDomainSQL); err != nil {
		p.logger.Error("failed to drop domain table", "error", err)
		return err
	}
	if _, err := conn.ExecContext(ctx, dropCounterSQL); err != nil {
		p.logger.Error("failed to drop counter table", "error", err)
		return err
	}
	return nil
}

// Prepare is used to prepare the internal queries
func (p *PGDatabase) Prepare() error {
	stmt, err := p.db.Prepare(upsertDomainSQL)
	if err != nil {
		return fmt.Errorf("failed to prepared query: %v", err)
	}
	p.upsertDomain = stmt

	stmt, err = p.db.Prepare(upsertCounterSQL)
	if err != nil {
		return fmt.Errorf("failed to prepared query: %v", err)
	}
	p.upsertCounter = stmt
	return nil
}

func (p *PGDatabase) UpsertDomain(attributes map[string]map[string]struct{}) error {
	// Flatten all the input pairs, skipping those in the cache
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
	for len(tuples) > 0 {
		var chunk []tuple
		if len(tuples) > TransactionSizeLimit {
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
		upsertStmt := tx.Stmt(p.upsertDomain)
		for _, tuple := range chunk {
			if _, err := upsertStmt.Exec(tuple.key, tuple.value); err != nil {
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

func (p *PGDatabase) UpsertCounters(counters []*ParsedKey) error {
	// Filter to only the counters that have changes
	var updates []*ParsedKey
	for _, c := range counters {
		lastCount, ok := p.counterCache.Get(c.Raw)
		if !ok || lastCount.(int64) != c.Count {
			updates = append(updates, c)
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
	for len(updates) > 0 {
		var chunk []*ParsedKey
		if len(updates) > TransactionSizeLimit {
			chunk = updates[:TransactionSizeLimit]
			updates = updates[TransactionSizeLimit:]
		} else {
			chunk = updates
			updates = nil
		}

		// Create a transaction
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			p.logger.Error("failed to start transaction", "error", err)
			return err
		}

		// Do all the updates in the transaction
		upsertStmt := tx.Stmt(p.upsertCounter)
		for _, c := range chunk {
			attrBytes, err := json.Marshal(c.Attributes)
			if err != nil {
				p.logger.Error("failed to marshal attributes", "attributes", c.Attributes, "error", err)
				return err
			}
			if _, err := upsertStmt.Exec(c.Interval, c.Date, attrBytes, c.Count); err != nil {
				p.logger.Error("failed to update counter table", "key", c.Raw,
					"count", c.Count, "error", err)
				return err
			}
		}

		// Commit all the updates
		if tx.Commit(); err != nil {
			p.logger.Error("failed to commit transaction", "error", err)
			return err
		}

		// Add to the cache
		for _, c := range chunk {
			p.counterCache.Add(c.Raw, c.Count)
		}
	}
	return nil
}

const (
	// upsertDomainSQL is used to upsert values into the domain table
	upsertDomainSQL = `INSERT INTO attributes_domain VALUES ($1, $2) ON CONFLICT DO NOTHING;`

	// upsertCounterSQL is used to upsert into the counters table
	upsertCounterSQL = `INSERT INTO counters (interval, date, attributes, count) VALUES ($1, $2, $3, $4) ON CONFLICT (interval, date, attributes) DO UPDATE SET count = GREATEST(EXCLUDED.count, counters.count);`

	// createExtension is used to greate the UUID extension if not available
	createExtension = `CREATE EXTENSION IF NOT EXISTS "uuid-ossp";`

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

	// dropDomainSQL is used to drop the domain attributes table
	dropDomainSQL = `DROP TABLE IF EXISTS attributes_domain;`

	// dropCounterSQL is used to drop the counters table
	dropCounterSQL = `DROP TABLE IF EXISTS counters;`
)
