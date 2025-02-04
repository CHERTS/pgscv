package collector

import (
	"database/sql"
	"testing"

	"github.com/cherts/pgscv/internal/model"
	"github.com/jackc/pgproto3/v2"
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
				Colnames: []pgproto3.FieldDescription{
					{Name: []byte("backend_type")}, {Name: []byte("object")}, {Name: []byte("context")},
					{Name: []byte("reads")}, {Name: []byte("read_time")}, {Name: []byte("writes")}, {Name: []byte("write_time")},
					{Name: []byte("writebacks")}, {Name: []byte("writeback_time")}, {Name: []byte("extends")}, {Name: []byte("extend_time")},
					{Name: []byte("op_bytes")}, {Name: []byte("hits")}, {Name: []byte("evictions")}, {Name: []byte("reuses")},
					{Name: []byte("fsyncs")}, {Name: []byte("fsync_time")},
				},
				Rows: [][]sql.NullString{
					{
						{String: "autovacuum launcher", Valid: true}, {String: "relation", Valid: true}, {String: "bulkread", Valid: true},
						{String: "0", Valid: true}, {String: "0", Valid: true}, {String: "0", Valid: true}, {String: "0", Valid: true},
						{String: "0", Valid: true}, {String: "0", Valid: true}, {String: "0", Valid: true}, {String: "0", Valid: true},
						{String: "8192", Valid: true}, {String: "0", Valid: true}, {String: "0", Valid: true}, {String: "0", Valid: true},
						{String: "0", Valid: true}, {String: "0", Valid: true},
					},
				},
			},
			want: map[string]postgresStatIO{
				"autovacuum launcher/relation/bulkread": {
					BackendType: "autovacuum launcher", IoObject: "testsrelationchema", IoContext: "bulkread",
					Reads: 0, ReadTime: 0, Writes: 0, WriteTime: 0,
					Writebacks: 0, WritebackTime: 0, Extends: 0, ExtendTime: 0,
					OpBytes: 8192, Hits: 0, Evictions: 0, Reuses: 0,
					Fsyncs: 0, FsyncTime: 0,
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
