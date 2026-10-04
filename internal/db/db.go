// Package db owns Postgres connectivity: pool construction and schema migrations.
package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// NewPool builds a connection pool. It does not dial: pgxpool connects lazily, which lets
// the HTTP server come up (and report "not ready") before the database is reachable.
//
// Session-level timeouts make every connection fail fast instead of piling up behind a
// stuck transaction:
//   - lock_timeout: a seat row lock is normally held for ~1ms; 5s means something is wrong.
//   - statement_timeout: hard ceiling for any single statement.
//   - idle_in_transaction_session_timeout: kills a transaction whose client went away.
func NewPool(ctx context.Context, url string, maxConns int32, appName string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse DATABASE_URL: %w", err)
	}
	cfg.MaxConns = maxConns
	cfg.MinConns = 0
	cfg.MaxConnIdleTime = 5 * time.Minute
	cfg.MaxConnLifetime = time.Hour
	cfg.HealthCheckPeriod = 30 * time.Second
	if cfg.ConnConfig.ConnectTimeout == 0 {
		cfg.ConnConfig.ConnectTimeout = 5 * time.Second
	}
	rp := cfg.ConnConfig.RuntimeParams
	rp["application_name"] = appName
	rp["lock_timeout"] = "5000"
	rp["statement_timeout"] = "10000"
	rp["idle_in_transaction_session_timeout"] = "15000"
	return pgxpool.NewWithConfig(ctx, cfg)
}

// PgCode returns the SQLSTATE of a Postgres error, or "" if err is not one.
func PgCode(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}
