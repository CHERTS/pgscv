package collector

import (
	"database/sql"
	"github.com/cherts/pgscv/internal/model"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPostgresReplicationCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"postgres_replication_lag_bytes",
			"postgres_replication_lag_all_bytes",
			"postgres_replication_lag_seconds",
			"postgres_replication_lag_all_seconds",
		},
		optional:  []string{},
		collector: NewPostgresReplicationCollector,
		service:   model.ServiceTypePostgresql,
	}

	pipeline(t, input)
}

func Test_parsePostgresReplicationStats(t *testing.T) {
	var testCases = []struct {
		name string
		res  *model.PGResult
		want map[string]postgresReplicationStat
	}{
		{
			name: "normal output",
			res: &model.PGResult{
				Nrows: 1,
				Ncols: 14,
				Colnames: []pgconn.FieldDescription{
					{Name: "pid"}, {Name: "client_addr"}, {Name: "client_port"}, {Name: "user"}, {Name: "application_name"}, {Name: "state"},
					{Name: "pending_lag_bytes"}, {Name: "write_lag_bytes"}, {Name: "flush_lag_bytes"},
					{Name: "replay_lag_bytes"}, {Name: "total_lag_bytes"}, {Name: "write_lag_seconds"},
					{Name: "flush_lag_seconds"}, {Name: "replay_lag_seconds"}, {Name: "total_lag_seconds"},
				},
				Rows: [][]sql.NullString{
					{
						{String: "123456", Valid: true}, {String: "127.0.0.1", Valid: true}, {String: "51658", Valid: true}, {String: "testuser", Valid: true}, {String: "testapp", Valid: true},
						{String: "teststate", Valid: true},
						{String: "100", Valid: true}, {String: "200", Valid: true}, {String: "300", Valid: true}, {String: "400", Valid: true},
						{String: "500", Valid: true}, {String: "600", Valid: true}, {String: "700", Valid: true}, {String: "800", Valid: true}, {String: "2100", Valid: true},
					},
					{
						// pg_receivewals and pg_basebackups don't have replay lag.
						{String: "101010", Valid: true}, {String: "127.0.0.1", Valid: true}, {String: "52441", Valid: true}, {String: "testuser", Valid: true}, {String: "pg_receivewal", Valid: true},
						{String: "teststate", Valid: true},
						{String: "4257", Valid: true}, {String: "8452", Valid: true}, {String: "5785", Valid: true}, {String: "", Valid: false},
						{String: "", Valid: false}, {String: "2458", Valid: true}, {String: "7871", Valid: true}, {String: "6896", Valid: true}, {String: "17225", Valid: true},
					},
				},
			},
			want: map[string]postgresReplicationStat{
				"123456": {
					pid: "123456", clientaddr: "127.0.0.1", clientport: "51658", user: "testuser", applicationName: "testapp", state: "teststate",
					values: map[string]float64{
						"pending_lag_bytes": 100, "write_lag_bytes": 200, "flush_lag_bytes": 300, "replay_lag_bytes": 400, "total_lag_bytes": 500,
						"write_lag_seconds": 600, "flush_lag_seconds": 700, "replay_lag_seconds": 800, "total_lag_seconds": 2100,
					},
				},
				"101010": {
					pid: "101010", clientaddr: "127.0.0.1", clientport: "52441", user: "testuser", applicationName: "pg_receivewal", state: "teststate",
					values: map[string]float64{
						"pending_lag_bytes": 4257, "write_lag_bytes": 8452, "flush_lag_bytes": 5785,
						"write_lag_seconds": 2458, "flush_lag_seconds": 7871, "replay_lag_seconds": 6896, "total_lag_seconds": 17225,
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := parsePostgresReplicationStats(tc.res, []string{"client_addr", "client_port", "user", "application_name", "state", "type"})
			assert.EqualValues(t, tc.want, got)
		})
	}
}

func Test_selectReplicationQuery(t *testing.T) {
	var testcases = []struct {
		version PostgresVersion
		want    string
	}{
		{version: PostgresVersion{Numeric: 90620, IsAwsAurora: false}, want: postgresReplicationQuery96},
		{version: PostgresVersion{Numeric: 90605, IsAwsAurora: false}, want: postgresReplicationQuery96},
		{version: PostgresVersion{Numeric: 100000, IsAwsAurora: false}, want: postgresReplicationQueryLatest},
		{version: PostgresVersion{Numeric: 150000, IsAwsAurora: false}, want: postgresReplicationQueryLatest},
		{version: PostgresVersion{Numeric: 170004, IsAwsAurora: false}, want: postgresReplicationQueryLatest},
		{version: PostgresVersion{Numeric: 170004, IsAwsAurora: true}, want: postgresAuroraReplicationQueryLatest},
	}

	for _, tc := range testcases {
		t.Run("", func(t *testing.T) {
			assert.Equal(t, tc.want, selectReplicationQuery(tc.version))
		})
	}
}
