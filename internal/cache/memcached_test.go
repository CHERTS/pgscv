package cache

import (
	"database/sql"
	"net"
	"testing"
	"time"

	"github.com/cherts/pgscv/internal/model"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func isMemcachedAvailable() bool {
	conn, err := net.DialTimeout("tcp", "localhost:11211", 1*time.Second)
	if err != nil {
		return false
	}
	defer conn.Close()
	return true
}

func TestMemcachedCache_Integration(t *testing.T) {
	if !isMemcachedAvailable() {
		t.Skip("Memcached not available on localhost:11211. Skipping integration tests.")
	}

	cache := NewMemcachedCache("localhost:11211", "test-commit")

	err := cache.Delete("test-key")
	if err != nil && err.Error() != "memcache: cache miss" {
		t.Logf("Warning: could not delete test key: %v", err)
	}

	testData := &model.PGResult{
		Nrows: 2,
		Ncols: 3,
		Colnames: []pgconn.FieldDescription{
			{Name: "id", DataTypeOID: 23},
			{Name: "name", DataTypeOID: 25},
			{Name: "value", DataTypeOID: 23},
		},
		Rows: [][]sql.NullString{
			{
				{String: "1", Valid: true},
				{String: "test", Valid: true},
				{String: "123", Valid: true},
			},
			{
				{String: "2", Valid: true},
				{String: "another", Valid: true},
				{String: "456", Valid: true},
			},
		},
	}

	t.Run("set and get operations", func(t *testing.T) {
		err := cache.Set("test-key", testData, 10*time.Second)
		require.NoError(t, err)

		result, _, err := cache.Get("test-key")
		require.NoError(t, err)
		require.NotNil(t, result)

		assert.Equal(t, testData.Nrows, result.Nrows)
		assert.Equal(t, testData.Ncols, result.Ncols)
		assert.Equal(t, len(testData.Colnames), len(result.Colnames))
		assert.Equal(t, len(testData.Rows), len(result.Rows))

		for i, col := range testData.Colnames {
			assert.Equal(t, col.Name, result.Colnames[i].Name)
			assert.Equal(t, col.DataTypeOID, result.Colnames[i].DataTypeOID)
		}

		for i, row := range testData.Rows {
			for j, cell := range row {
				assert.Equal(t, cell.String, result.Rows[i][j].String)
				assert.Equal(t, cell.Valid, result.Rows[i][j].Valid)
			}
		}
	})

	t.Run("get non-existent key", func(t *testing.T) {
		result, _, err := cache.Get("non-existent-key")
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "cache miss")
	})

	t.Run("delete operation", func(t *testing.T) {
		err := cache.Set("delete-test-key", testData, 10*time.Second)
		require.NoError(t, err)

		result, _, err := cache.Get("delete-test-key")
		require.NoError(t, err)
		assert.NotNil(t, result)

		err = cache.Delete("delete-test-key")
		require.NoError(t, err)

		result, _, err = cache.Get("delete-test-key")
		assert.Error(t, err)
		assert.Nil(t, result)
	})

	t.Run("TTL expiration", func(t *testing.T) {
		err := cache.Set("ttl-test-key", testData, 2*time.Second)
		require.NoError(t, err)

		result, _, err := cache.Get("ttl-test-key")
		require.NoError(t, err)
		assert.NotNil(t, result)

		time.Sleep(3 * time.Second)

		result, _, err = cache.Get("ttl-test-key")
		assert.Error(t, err)
		assert.Nil(t, result)
	})

	t.Run("hash generation", func(t *testing.T) {
		hash := cache.Hash("arg1", "arg2", 123, "test")
		assert.NotEmpty(t, hash)
		assert.Len(t, hash, 64) // SHA256 hash length

		hash2 := cache.Hash("arg1", "arg2", 123, "test")
		assert.Equal(t, hash, hash2)

		hash3 := cache.Hash("arg1", "arg2", 124, "test")
		assert.NotEqual(t, hash, hash3)
	})

	t.Run("large TTL error", func(t *testing.T) {
		err := cache.Set("large-ttl-key", testData, time.Duration(1<<31)*time.Second)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "TTL must be between")
	})

	t.Run("test with NULL values", func(t *testing.T) {
		nullData := &model.PGResult{
			Nrows: 1,
			Ncols: 2,
			Colnames: []pgconn.FieldDescription{
				{Name: "id", DataTypeOID: 23},
				{Name: "data", DataTypeOID: 25},
			},
			Rows: [][]sql.NullString{
				{
					{String: "1", Valid: true},
					{String: "", Valid: false}, // NULL
				},
			},
		}

		err := cache.Set("null-test-key", nullData, 10*time.Second)
		require.NoError(t, err)

		result, _, err := cache.Get("null-test-key")
		require.NoError(t, err)
		require.NotNil(t, result)

		assert.False(t, result.Rows[0][1].Valid)
		assert.Equal(t, "", result.Rows[0][1].String)
	})

	t.Run("test empty Result", func(t *testing.T) {
		emptyData := &model.PGResult{
			Nrows:    0,
			Ncols:    0,
			Colnames: []pgconn.FieldDescription{},
			Rows:     [][]sql.NullString{},
		}

		err := cache.Set("empty-test-key", emptyData, 10*time.Second)
		require.NoError(t, err)

		result, _, err := cache.Get("empty-test-key")
		require.NoError(t, err)
		require.NotNil(t, result)

		assert.Equal(t, 0, result.Nrows)
		assert.Equal(t, 0, result.Ncols)
		assert.Empty(t, result.Colnames)
		assert.Empty(t, result.Rows)
	})
}

func TestMemcachedCache_MultipleServers(t *testing.T) {
	if !isMemcachedAvailable() {
		t.Skip("Memcached not available. Skipping multiple servers test.")
	}

	cache := NewMemcachedCache("localhost:11211 ,  127.0.0.1:11211 ", "test-commit")

	testData := &model.PGResult{
		Nrows: 1,
		Ncols: 1,
		Colnames: []pgconn.FieldDescription{
			{Name: "key", DataTypeOID: 25},
		},
		Rows: [][]sql.NullString{
			{{String: "value", Valid: true}},
		},
	}

	err := cache.Set("multi-server-test", testData, 5*time.Second)
	require.NoError(t, err)

	result, _, err := cache.Get("multi-server-test")
	require.NoError(t, err)
	assert.NotNil(t, result)
}

func TestMemcachedCache_ConnectionErrors(t *testing.T) {
	cache := NewMemcachedCache("nonexistent-host:11211", "test-commit")

	testData := &model.PGResult{
		Nrows: 1,
		Ncols: 1,
		Colnames: []pgconn.FieldDescription{
			{Name: "key", DataTypeOID: 25},
		},
		Rows: [][]sql.NullString{
			{{String: "value", Valid: true}},
		},
	}

	err := cache.Set("test-key", testData, 5*time.Second)
	assert.Error(t, err)

	_, _, err = cache.Get("test-key")
	assert.Error(t, err)

	err = cache.Delete("test-key")
	assert.Error(t, err)
}
