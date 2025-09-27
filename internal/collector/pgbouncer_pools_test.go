package collector

import (
	"database/sql"
	"github.com/cherts/pgscv/internal/model"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPgbouncerPoolsCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"pgbouncer_pool_connections_in_flight",
			"pgbouncer_pool_max_wait_seconds",
			"pgbouncer_client_connections_in_flight",
		},
		collector: NewPgbouncerPoolsCollector,
		service:   model.ServiceTypePgbouncer,
	}

	pipeline(t, input)
}

func Test_parsePgbouncerPoolsStats(t *testing.T) {
	var testCases = []struct {
		name string
		res  *model.PGResult
		want map[string]pgbouncerPoolStat
	}{
		{
			name: "normal output",
			res: &model.PGResult{
				Nrows: 2,
				Ncols: 11,
				Colnames: []pgconn.FieldDescription{
					{Name: "database"}, {Name: "user"},
					{Name: "cl_active"}, {Name: "cl_waiting"}, {Name: "sv_active"}, {Name: "sv_idle"},
					{Name: "sv_used"}, {Name: "sv_tested"}, {Name: "sv_login"}, {Name: "maxwait"},
					{Name: "pool_mode"},
				},
				Rows: [][]sql.NullString{
					{
						{String: "testdb1", Valid: true}, {String: "testuser1", Valid: true},
						{String: "15", Valid: true}, {String: "5", Valid: true}, {String: "10", Valid: true}, {String: "1", Valid: true},
						{String: "1", Valid: true}, {String: "1", Valid: true}, {String: "1", Valid: true}, {String: "1", Valid: true},
						{String: "transaction", Valid: true},
					},
					{
						{String: "testdb2", Valid: true}, {String: "testuser2", Valid: true},
						{String: "25", Valid: true}, {String: "10", Valid: true}, {String: "25", Valid: true}, {String: "2", Valid: true},
						{String: "2", Valid: true}, {String: "2", Valid: true}, {String: "2", Valid: true}, {String: "2", Valid: true},
						{String: "statement", Valid: true},
					},
				},
			},
			want: map[string]pgbouncerPoolStat{
				"testuser1/testdb1/transaction": {
					database: "testdb1", user: "testuser1", clActive: 15, clWaiting: 5, svActive: 10, svIdle: 1, svUsed: 1, svTested: 1, svLogin: 1, maxWait: 1, mode: "transaction",
				},
				"testuser2/testdb2/statement": {
					database: "testdb2", user: "testuser2", clActive: 25, clWaiting: 10, svActive: 25, svIdle: 2, svUsed: 2, svTested: 2, svLogin: 2, maxWait: 2, mode: "statement",
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := parsePgbouncerPoolsStats(tc.res, []string{"database", "user", "pool_mode"})
			assert.EqualValues(t, tc.want, got)
		})
	}
}

func Test_parsePgbouncerClientsStats(t *testing.T) {
	var testCases = []struct {
		name string
		res  *model.PGResult
		want map[string]float64
	}{
		{
			name: "normal output",
			res: &model.PGResult{
				Nrows: 10,
				Ncols: 5,
				Colnames: []pgconn.FieldDescription{
					{Name: "user"}, {Name: "database"}, {Name: "addr"}, {Name: "state"}, {Name: "port"},
				},
				Rows: [][]sql.NullString{
					{{String: "user1", Valid: true}, {String: "db1", Valid: true}, {String: "1.1.1.1", Valid: true}, {String: "active", Valid: true}, {String: "11", Valid: true}},
					{{String: "user2", Valid: true}, {String: "db2", Valid: true}, {String: "2.2.2.2", Valid: true}, {String: "idle", Valid: true}, {String: "22", Valid: true}},
					{{String: "user1", Valid: true}, {String: "db1", Valid: true}, {String: "1.1.1.1", Valid: true}, {String: "active", Valid: true}, {String: "12", Valid: true}},
					{{String: "user3", Valid: true}, {String: "db3", Valid: true}, {String: "unix", Valid: true}, {String: "active", Valid: true}, {String: "unix", Valid: true}},
					{{String: "user3", Valid: true}, {String: "db3", Valid: true}, {String: "unix", Valid: true}, {String: "idle", Valid: true}, {String: "unix", Valid: true}},
					{{String: "user2", Valid: true}, {String: "db2", Valid: true}, {String: "2.2.2.2", Valid: true}, {String: "active", Valid: true}, {String: "23", Valid: true}},
					{{String: "user1", Valid: true}, {String: "db1", Valid: true}, {String: "1.1.1.1", Valid: true}, {String: "active", Valid: true}, {String: "13", Valid: true}},
					{{String: "user1", Valid: true}, {String: "db1", Valid: true}, {String: "1.1.1.1", Valid: true}, {String: "idle", Valid: true}, {String: "14", Valid: true}},
					{{String: "user2", Valid: true}, {String: "db2", Valid: true}, {String: "2.2.2.2", Valid: true}, {String: "active", Valid: true}, {String: "24", Valid: true}},
					{{String: "user1", Valid: true}, {String: "db1", Valid: true}, {String: "1.1.1.1", Valid: true}, {String: "active", Valid: true}, {String: "25", Valid: true}},
				},
			},
			want: map[string]float64{
				"user1/db1/1.1.1.1": 5,
				"user2/db2/2.2.2.2": 3,
				"user3/db3/unix":    2,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := parsePgbouncerClientsStats(tc.res)
			assert.EqualValues(t, tc.want, got)
		})
	}
}
