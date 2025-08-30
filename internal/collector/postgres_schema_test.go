package collector

import (
	"context"
	"github.com/cherts/pgscv/internal/model"
	"github.com/cherts/pgscv/internal/store"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPostgresSchemaCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"postgres_schema_system_catalog_bytes",
			"postgres_schema_non_pk_tables",
			"postgres_schema_invalid_indexes_bytes",
			"postgres_schema_non_indexed_fkeys",
			"postgres_schema_redundant_indexes_bytes",
			"postgres_schema_sequence_exhaustion_ratio",
			"postgres_schema_mistyped_fkeys",
		},
		collector: NewPostgresSchemasCollector,
		service:   model.ServiceTypePostgresql,
	}

	pipeline(t, input)
}

func Test_getSystemCatalogSize(t *testing.T) {
	conn := store.NewTest(t)
	got, err := getSystemCatalogSize(context.Background(), Config{DB: conn}, nil)
	assert.NoError(t, err)
	assert.NotEqual(t, float64(0), got)

	conn.Conn().Close()
	got, err = getSystemCatalogSize(context.Background(), Config{DB: conn}, nil)
	assert.Error(t, err)
	assert.Equal(t, float64(0), got)
}

func Test_getSchemaNonPKTables(t *testing.T) {
	conn := store.NewTest(t)
	got, err := getSchemaNonPKTables(context.Background(), Config{DB: conn}, nil)
	assert.NoError(t, err)
	assert.Less(t, 0, len(got))

	conn.Conn().Close()
	got, err = getSchemaNonPKTables(context.Background(), Config{DB: conn}, nil)
	assert.Error(t, err)
	assert.Equal(t, 0, len(got))
}

func Test_getSchemaInvalidIndexes(t *testing.T) {
	conn := store.NewTest(t)
	got, err := getSchemaInvalidIndexes(context.Background(), Config{DB: conn}, nil)
	assert.NoError(t, err)
	assert.Less(t, 0, len(got))

	conn.Conn().Close()
	got, err = getSchemaInvalidIndexes(context.Background(), Config{DB: conn}, nil)
	assert.Error(t, err)
	assert.Equal(t, 0, len(got))
}

func Test_getSchemaNonIndexedFK(t *testing.T) {
	conn := store.NewTest(t)
	got, err := getSchemaNonIndexedFK(context.Background(), Config{DB: conn}, nil)
	assert.NoError(t, err)
	assert.Less(t, 0, len(got))

	conn.Conn().Close()
	got, err = getSchemaNonIndexedFK(context.Background(), Config{DB: conn}, nil)
	assert.Error(t, err)
	assert.Equal(t, 0, len(got))
}

func Test_getSchemaRedundantIndexes(t *testing.T) {
	conn := store.NewTest(t)
	got, err := getSchemaRedundantIndexes(context.Background(), Config{DB: conn}, nil)
	assert.NoError(t, err)
	assert.Less(t, 0, len(got))

	conn.Conn().Close()
	got, err = getSchemaRedundantIndexes(context.Background(), Config{DB: conn}, nil)
	assert.Error(t, err)
	assert.Equal(t, 0, len(got))
}

func Test_getSchemaSequences(t *testing.T) {
	conn := store.NewTest(t)
	got, err := getSchemaSequences(context.Background(), Config{DB: conn}, nil)
	assert.NoError(t, err)
	assert.Less(t, 0, len(got))

	conn.Conn().Close()
	got, err = getSchemaSequences(context.Background(), Config{DB: conn}, nil)
	assert.Error(t, err)
	assert.Equal(t, 0, len(got))
}

func Test_getSchemaFKDatatypeMismatch(t *testing.T) {
	conn := store.NewTest(t)
	got, err := getSchemaFKDatatypeMismatch(context.Background(), Config{DB: conn}, nil)
	assert.NoError(t, err)
	assert.Less(t, 0, len(got))

	conn.Conn().Close()
	got, err = getSchemaFKDatatypeMismatch(context.Background(), Config{DB: conn}, nil)
	assert.Error(t, err)
	assert.Equal(t, 0, len(got))
}
