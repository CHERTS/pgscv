package collector

import (
	"database/sql"
	"fmt"
	"github.com/jackc/pgx/v5/pgconn"
	"testing"

	"github.com/cherts/pgscv/internal/model"
	"github.com/stretchr/testify/assert"
)

func TestPostgresStatementsCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"postgres_statements_query_info",
			"postgres_statements_calls_total",
			"postgres_statements_rows_total",
			"postgres_statements_time_seconds_total",
			"postgres_statements_time_seconds_all_total",
		},
		optional: []string{
			"postgres_statements_shared_buffers_hit_total",
			"postgres_statements_shared_buffers_read_bytes_total",
			"postgres_statements_shared_buffers_dirtied_total",
			"postgres_statements_shared_buffers_written_bytes_total",
			"postgres_statements_local_buffers_hit_total",
			"postgres_statements_local_buffers_read_bytes_total",
			"postgres_statements_local_buffers_dirtied_total",
			"postgres_statements_local_buffers_written_bytes_total",
			"postgres_statements_temp_read_bytes_total",
			"postgres_statements_temp_written_bytes_total",
			"postgres_statements_wal_records_total",
			"postgres_statements_wal_bytes_all_total",
			"postgres_statements_wal_bytes_total",
		},
		collector: NewPostgresStatementsCollector,
		service:   model.ServiceTypePostgresql,
	}

	pipeline(t, input)
}

func Test_parsePostgresStatementsStats(t *testing.T) {
	var testCases = []struct {
		name string
		res  *model.PGResult
		want map[string]postgresStatementStat
	}{
		{
			name: "normal output, Postgres 12",
			res: &model.PGResult{
				Nrows: 1,
				Ncols: 19,
				Colnames: []pgconn.FieldDescription{
					{Name: "database"}, {Name: "user"}, {Name: "queryid"}, {Name: "query"},
					{Name: "calls"}, {Name: "rows"},
					{Name: "total_time"}, {Name: "blk_read_time"}, {Name: "blk_write_time"},
					{Name: "shared_blks_hit"}, {Name: "shared_blks_read"}, {Name: "shared_blks_dirtied"}, {Name: "shared_blks_written"},
					{Name: "local_blks_hit"}, {Name: "local_blks_read"}, {Name: "local_blks_dirtied"}, {Name: "local_blks_written"},
					{Name: "temp_blks_read"}, {Name: "temp_blks_written"},
				},
				Rows: [][]sql.NullString{
					{
						{String: "testdb", Valid: true}, {String: "testuser", Valid: true}, {String: "example_queryid", Valid: true}, {String: "SELECT test", Valid: true},
						{String: "1000", Valid: true}, {String: "2000", Valid: true},
						{String: "30000", Valid: true}, {String: "6000", Valid: true}, {String: "4000", Valid: true},
						{String: "100", Valid: true}, {String: "110", Valid: true}, {String: "120", Valid: true}, {String: "130", Valid: true},
						{String: "500", Valid: true}, {String: "510", Valid: true}, {String: "520", Valid: true}, {String: "530", Valid: true},
						{String: "700", Valid: true}, {String: "710", Valid: true},
					},
				},
			},
			want: map[string]postgresStatementStat{
				"testdb/testuser/example_queryid": {
					database: "testdb", user: "testuser", queryid: "example_queryid", query: "SELECT test",
					calls: 1000, rows: 2000,
					totalExecTime: 30000, blkReadTime: 6000, blkWriteTime: 4000,
					sharedBlksHit: 100, sharedBlksRead: 110, sharedBlksDirtied: 120, sharedBlksWritten: 130,
					localBlksHit: 500, localBlksRead: 510, localBlksDirtied: 520, localBlksWritten: 530,
					tempBlksRead: 700, tempBlksWritten: 710,
				},
			},
		},
		{
			name: "normal output, Postgres 13",
			res: &model.PGResult{
				Nrows: 1,
				Ncols: 23,
				Colnames: []pgconn.FieldDescription{
					{Name: "database"}, {Name: "user"}, {Name: "queryid"}, {Name: "query"},
					{Name: "calls"}, {Name: "rows"},
					{Name: "total_exec_time"}, {Name: "total_plan_time"}, {Name: "blk_read_time"}, {Name: "blk_write_time"},
					{Name: "shared_blks_hit"}, {Name: "shared_blks_read"}, {Name: "shared_blks_dirtied"}, {Name: "shared_blks_written"},
					{Name: "local_blks_hit"}, {Name: "local_blks_read"}, {Name: "local_blks_dirtied"}, {Name: "local_blks_written"},
					{Name: "temp_blks_read"}, {Name: "temp_blks_written"}, {Name: "wal_records"}, {Name: "wal_fpi"},
					{Name: "wal_bytes"},
				},
				Rows: [][]sql.NullString{
					{
						{String: "testdb", Valid: true}, {String: "testuser", Valid: true}, {String: "example_queryid", Valid: true}, {String: "SELECT test", Valid: true},
						{String: "1000", Valid: true}, {String: "2000", Valid: true},
						{String: "30000", Valid: true}, {String: "100", Valid: true}, {String: "6000", Valid: true}, {String: "4000", Valid: true},
						{String: "100", Valid: true}, {String: "110", Valid: true}, {String: "120", Valid: true}, {String: "130", Valid: true},
						{String: "500", Valid: true}, {String: "510", Valid: true}, {String: "520", Valid: true}, {String: "530", Valid: true},
						{String: "700", Valid: true}, {String: "710", Valid: true}, {String: "720", Valid: true}, {String: "730", Valid: true},
						{String: "740", Valid: true},
					},
				},
			},
			want: map[string]postgresStatementStat{
				"testdb/testuser/example_queryid": {
					database: "testdb", user: "testuser", queryid: "example_queryid", query: "SELECT test",
					calls: 1000, rows: 2000,
					totalExecTime: 30000, totalPlanTime: 100, blkReadTime: 6000, blkWriteTime: 4000,
					sharedBlksHit: 100, sharedBlksRead: 110, sharedBlksDirtied: 120, sharedBlksWritten: 130,
					localBlksHit: 500, localBlksRead: 510, localBlksDirtied: 520, localBlksWritten: 530,
					tempBlksRead: 700, tempBlksWritten: 710, walRecords: 720, walFPI: 730, walBytes: 740,
				},
			},
		},
		{
			name: "lot of nulls and unknown columns",
			res: &model.PGResult{
				Nrows: 1,
				Ncols: 21,
				Colnames: []pgconn.FieldDescription{
					{Name: "database"}, {Name: "user"}, {Name: "queryid"}, {Name: "query"},
					{Name: "calls"}, {Name: "rows"},
					{Name: "total_exec_time"}, {Name: "total_plan_time"}, {Name: "blk_read_time"}, {Name: "blk_write_time"}, {Name: "min_time"},
					{Name: "shared_blks_hit"}, {Name: "shared_blks_read"}, {Name: "shared_blks_dirtied"}, {Name: "shared_blks_written"},
					{Name: "local_blks_hit"}, {Name: "local_blks_read"}, {Name: "local_blks_dirtied"}, {Name: "local_blks_written"},
					{Name: "temp_blks_read"}, {Name: "temp_blks_written"},
				},
				Rows: [][]sql.NullString{
					{
						{String: "testdb", Valid: true}, {String: "testuser", Valid: true}, {String: "example_queryid", Valid: true}, {String: "SELECT test", Valid: true},
						{String: "1000", Valid: true}, {String: "2000", Valid: true},
						{String: "30000", Valid: true}, {String: "100", Valid: true}, {String: "6000", Valid: true}, {String: "4000", Valid: true}, {String: "100", Valid: true},
						{}, {}, {}, {}, {}, {}, {}, {}, {}, {},
					},
				},
			},
			want: map[string]postgresStatementStat{
				"testdb/testuser/example_queryid": {
					database: "testdb", user: "testuser", queryid: "example_queryid", query: "SELECT test",
					calls: 1000, rows: 2000,
					totalExecTime: 30000, totalPlanTime: 100, blkReadTime: 6000, blkWriteTime: 4000,
					sharedBlksHit: 0, sharedBlksRead: 0, sharedBlksDirtied: 0, sharedBlksWritten: 0,
					localBlksHit: 0, localBlksRead: 0, localBlksDirtied: 0, localBlksWritten: 0,
					tempBlksRead: 0, tempBlksWritten: 0,
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := parsePostgresStatementsStats(tc.res, []string{"usename", "datname", "queryid", "query"})
			assert.EqualValues(t, tc.want, got)
		})
	}
}

func Test_selectStatementsQuery(t *testing.T) {
	testcases := []struct {
		version int
		want    string
		topK    int
	}{
		{version: PostgresV12, want: fmt.Sprintf(postgresStatementsQuery12, "p.query", "example"), topK: 0},
		{version: PostgresV12, want: fmt.Sprintf(postgresStatementsQuery12TopK, "p.query", "example"), topK: 100},
		{version: PostgresV13, want: fmt.Sprintf(postgresStatementsQuery16, "p.query", "example"), topK: 0},
		{version: PostgresV13, want: fmt.Sprintf(postgresStatementsQuery16TopK, "p.query", "example"), topK: 100},
		{version: PostgresV17, want: fmt.Sprintf(postgresStatementsQuery17, "p.query", "example"), topK: 0},
		{version: PostgresV17, want: fmt.Sprintf(postgresStatementsQuery17TopK, "p.query", "example"), topK: 100},
		{version: PostgresV18, want: fmt.Sprintf(postgresStatementsQueryLatest, "p.query", "example"), topK: 0},
		{version: PostgresV18, want: fmt.Sprintf(postgresStatementsQueryLatestTopK, "p.query", "example"), topK: 100},
	}

	for _, tc := range testcases {
		assert.Equal(t, tc.want, selectStatementsQuery(tc.version, "example", false, tc.topK))
	}
}
