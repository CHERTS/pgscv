package collector

import (
	"database/sql"
	"github.com/jackc/pgx/v5/pgconn"
	"testing"

	"github.com/cherts/pgscv/internal/model"
	"github.com/stretchr/testify/assert"
)

func TestPostgresStatIOCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"postgres_stat_io_reads",
			"postgres_stat_io_read_time",
			"postgres_stat_io_writes",
			"postgres_stat_io_write_time",
			"postgres_stat_io_writebacks",
			"postgres_stat_io_writeback_time",
			"postgres_stat_io_extends",
			"postgres_stat_io_extend_time",
			"postgres_stat_io_hits",
			"postgres_stat_io_evictions",
			"postgres_stat_io_reuses",
			"postgres_stat_io_fsyncs",
			"postgres_stat_io_fsync_time",
			"postgres_stat_io_read_bytes",
			"postgres_stat_io_write_bytes",
			"postgres_stat_io_extend_bytes",
		},
		collector: NewPostgresStatIOCollector,
		service:   model.ServiceTypePostgresql,
	}

	pipeline(t, input)
}

func Test_parsePostgresStatIO(t *testing.T) {
	var testCases = []struct {
		name string
		res  *model.PGResult
		want map[string]postgresStatIO
	}{
		{
			name: "normal output, Postgres 16",
			res: &model.PGResult{
				Nrows: 1,
				Ncols: 17,
				Colnames: []pgconn.FieldDescription{
					{Name: "backend_type"}, {Name: "object"}, {Name: "context"},
					{Name: "reads"}, {Name: "read_time"}, {Name: "writes"}, {Name: "write_time"},
					{Name: "writebacks"}, {Name: "writeback_time"}, {Name: "extends"}, {Name: "extend_time"},
					{Name: "hits"}, {Name: "evictions"}, {Name: "reuses"},
					{Name: "fsyncs"}, {Name: "fsync_time"},
					{Name: "read_bytes"}, {Name: "write_bytes"}, {Name: "extend_bytes"},
				},
				Rows: [][]sql.NullString{
					{
						{String: "autovacuum launcher", Valid: true}, {String: "relation", Valid: true}, {String: "bulkread", Valid: true},
						{String: "0", Valid: true}, {String: "0", Valid: true}, {String: "0", Valid: true}, {String: "0", Valid: true},
						{String: "0", Valid: true}, {String: "0", Valid: true}, {String: "0", Valid: true}, {String: "0", Valid: true},
						{String: "0", Valid: true}, {String: "0", Valid: true}, {String: "0", Valid: true},
						{String: "0", Valid: true}, {String: "0", Valid: true},
						{String: "0", Valid: true}, {String: "0", Valid: true}, {String: "0", Valid: true},
					},
				},
			},
			want: map[string]postgresStatIO{
				"autovacuum launcher/relation/bulkread": {
					BackendType: "autovacuum launcher", IoObject: "relation", IoContext: "bulkread",
					Reads: 0, ReadTime: 0, Writes: 0, WriteTime: 0,
					Writebacks: 0, WritebackTime: 0, Extends: 0, ExtendTime: 0,
					Hits: 0, Evictions: 0, Reuses: 0,
					Fsyncs: 0, FsyncTime: 0,
					ReadBytes: 0, WriteBytes: 0, ExtendBytes: 0,
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := parsePostgresStatIO(tc.res, []string{"backend_type", "object", "context"})
			assert.EqualValues(t, tc.want, got)
		})
	}
}

func Test_selectStatIOQuery(t *testing.T) {
	var testcases = []struct {
		version int
		want    string
	}{
		{version: 160000, want: postgresStatIoQuery17},
		{version: 170000, want: postgresStatIoQuery17},
		{version: 180000, want: postgresStatIoQueryLatest},
	}

	for _, tc := range testcases {
		t.Run("", func(t *testing.T) {
			assert.Equal(t, tc.want, selectStatIOQuery(tc.version))
		})
	}
}
