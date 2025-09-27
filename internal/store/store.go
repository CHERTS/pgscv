// Package store is a pgSCV database helper
package store

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"time"

	"github.com/cherts/pgscv/internal/log"
	"github.com/cherts/pgscv/internal/model"
	"github.com/jackc/pgx/v5"
)

const (
	// Data types supported by parser of query results.
	dataTypeBool uint32 = 16
	// dataTypeChar uint32 = 18 is not supported - its conversion to sql.NullString lead to panic 'pgx' driver.
	dataTypeName    uint32 = 19
	dataTypeInt8    uint32 = 20
	dataTypeInt2    uint32 = 21
	dataTypeInt4    uint32 = 23
	dataTypeText    uint32 = 25
	dataTypeOid     uint32 = 26
	dataTypeFloat4  uint32 = 700
	dataTypeFloat8  uint32 = 701
	dataTypeInet    uint32 = 869
	dataTypeBpchar  uint32 = 1042
	dataTypeVarchar uint32 = 1043
	dataTypeNumeric uint32 = 1700
)

// DB is the database representation
type DB struct {
	conn *pgxpool.Pool // database connection object
}

// New creates new connection to Postgres/Pgbouncer using passed DSN
func New(connString string, connTimeout int) (*DB, error) {
	config, err := pgxpool.ParseConfig(connString)

	if err != nil {
		return nil, err
	}
	if connTimeout > 0 {
		config.ConnConfig.ConnectTimeout = time.Duration(connTimeout) * time.Second
	}

	return NewWithConfig(config)
}

// NewWithConfig creates new pool connections to Postgres/Pgbouncer using passed Config.
func NewWithConfig(config *pgxpool.Config) (*DB, error) {
	// Enable simple protocol for compatibility with Pgbouncer.
	config.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	// Using simple protocol requires explicit options to be set.
	config.ConnConfig.RuntimeParams = map[string]string{
		"standard_conforming_strings": "on",
		"client_encoding":             "UTF8",
	}

	pool, err := pgxpool.NewWithConfig(context.TODO(), config)

	if err != nil {
		return nil, err
	}

	return &DB{conn: pool}, nil
}

/* public db methods */

// Query is a wrapper on private query() method.
func (db *DB) Query(ctx context.Context, query string, args ...any) (*model.PGResult, error) {
	if db == nil {
		// @todo: debug
		return nil, fmt.Errorf("db is nil")
	}
	return db.query(ctx, query, args...)
}

// Close is wrapper on private close() method.
func (db *DB) Close() { db.close() }

// Conn provides access to public methods of *pgxpool.Pool struct
func (db *DB) Conn() *pgxpool.Pool { return db.conn }

/* private db methods */

// Query method executes passed query and wraps result into model.PGResult struct.
func (db *DB) query(ctx context.Context, query string, args ...any) (*model.PGResult, error) {

	if db.conn == nil {
		return nil, fmt.Errorf("db conn is nil")
	}
	rows, err := db.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	descriptions := rows.FieldDescriptions()

	var (
		ncols = len(descriptions)
		nrows int
	)
	colnames := make([]pgconn.FieldDescription, ncols)
	copy(colnames, descriptions)

	// Not the all data types could be safely converted into sql.NullString
	// and conversion errors lead to panic.
	// Check the data types are safe in returned result.
	for _, c := range colnames {
		if !isDataTypeSupported(c.DataTypeOID) {
			return nil, fmt.Errorf("query '%s', unsupported data type OID: %d", query, c.DataTypeOID)
		}
	}

	// Storage used for data extracted from rows.
	// Scan operation supports only slice of interfaces, 'pointers' slice is the intermediate store where all values written.
	// Next values from 'pointers' associated with type-strict slice - 'values'. When Scan is writing to the 'pointers' it
	// also writing to the 'values' under the hood. When all pointers/values have been scanned, put them into 'rowsStore'.
	// Finally we get queryResult iterable store with data and information about stored rows, columns and columns names.
	var rowsStore = make([][]sql.NullString, 0, 10)
	// TODO: refactor to pgx.CollectRows(rows)
	for rows.Next() {
		pointers := make([]any, ncols)
		values := make([]sql.NullString, ncols)

		for i := range pointers {
			pointers[i] = &values[i]
		}

		err = rows.Scan(pointers...)
		if err != nil {
			log.Warnf("skip collecting stats: %s", err)
			continue
		}
		rowsStore = append(rowsStore, values)
		nrows++
	}

	return &model.PGResult{
		Nrows:    nrows,
		Ncols:    ncols,
		Colnames: colnames,
		Rows:     rowsStore,
	}, nil
}

// Close method closes database connections gracefully.
func (db *DB) close() {
	db.Conn().Close()
}

// isDataTypeSupported tests passed type OID is supported.
func isDataTypeSupported(t uint32) bool {
	switch t {
	case dataTypeName, dataTypeBpchar, dataTypeVarchar, dataTypeText,
		dataTypeInt2, dataTypeInt4, dataTypeInt8,
		dataTypeOid, dataTypeFloat4, dataTypeFloat8, dataTypeNumeric,
		dataTypeBool, dataTypeInet:
		return true
	default:
		return false
	}
}

// Databases returns slice with databases names
func Databases(ctx context.Context, db *DB) ([]string, error) {
	// getDBList returns the list of databases that allowed for connection
	rows, err := db.Conn().Query(ctx,
		`SELECT datname FROM pg_database
			 WHERE NOT datistemplate AND datallowconn
			  AND has_database_privilege(datname, 'CONNECT')
			  AND NOT (version() LIKE '%yandex%' AND datname = 'postgres');`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var list = make([]string, 0, 10)
	for rows.Next() {
		var dbname string
		if err := rows.Scan(&dbname); err != nil {
			return nil, err
		}
		list = append(list, dbname)
	}
	return list, nil
}
