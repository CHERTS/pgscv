package cache

import (
	"database/sql"
	"github.com/cherts/pgscv/internal/model"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestInMemoryCache_BasicOperations(t *testing.T) {
	cache := NewInMemoryCache()

	testData := &model.PGResult{
		Nrows: 1,
		Ncols: 2,
		Colnames: []pgconn.FieldDescription{
			{Name: "id", DataTypeOID: 23},
			{Name: "name", DataTypeOID: 25},
		},
		Rows: [][]sql.NullString{
			{
				{String: "1", Valid: true},
				{String: "test", Valid: true},
			},
		},
	}

	t.Run("set and get", func(t *testing.T) {
		err := cache.Set("test-key", testData, 0)
		assert.NoError(t, err)

		result, _, err := cache.Get("test-key")
		assert.NoError(t, err)
		assert.Equal(t, testData.Nrows, result.Nrows)
		assert.Equal(t, testData.Ncols, result.Ncols)
		assert.Equal(t, testData.Rows[0][0].String, result.Rows[0][0].String)
	})

	t.Run("get non-existent key", func(t *testing.T) {
		result, _, err := cache.Get("non-existent")
		assert.Error(t, err)
		assert.Nil(t, result)
	})

	t.Run("delete key", func(t *testing.T) {
		err := cache.Set("to-delete", testData, 0)
		assert.NoError(t, err)

		err = cache.Delete("to-delete")
		assert.NoError(t, err)

		result, _, err := cache.Get("to-delete")
		assert.Error(t, err)
		assert.Nil(t, result)
	})
}

func TestInMemoryCache_NullValues(t *testing.T) {
	cache := NewInMemoryCache()

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
				{String: "", Valid: false},
			},
		},
	}

	err := cache.Set("null-test", nullData, 0)
	assert.NoError(t, err)

	result, _, err := cache.Get("null-test")
	assert.NoError(t, err)

	assert.False(t, result.Rows[0][1].Valid)
	assert.Equal(t, "", result.Rows[0][1].String)
}
