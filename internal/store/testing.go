package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestPostgresConnStr PostgreSQL connection string
const TestPostgresConnStr = "host=127.0.0.1 port=5432 user=pgscv dbname=pgscv_fixtures sslmode=disable"

// TestPgbouncerConnStr Pgbouncer connection string
const TestPgbouncerConnStr = "host=127.0.0.1 port=6432 user=pgscv dbname=pgbouncer sslmode=disable password=pgscv"

// NewTest create PostgreSQL test
func NewTest(t *testing.T) *DB {
	db, err := New(TestPostgresConnStr)
	assert.NoError(t, err)
	return db
}

// NewTestPgbouncer create Pgbouncer test
func NewTestPgbouncer(t *testing.T) *DB {
	db, err := New(TestPgbouncerConnStr)
	assert.NoError(t, err)
	return db
}
