package collector

import (
	"database/sql"
	"github.com/cherts/pgscv/internal/model"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPostgresConflictsCollector_Update(t *testing.T) {
	var input = pipelineInput{
		optional: []string{
			"postgres_recovery_conflicts_total",
		},
		collector: NewPostgresConflictsCollector,
		service:   model.ServiceTypePostgresql,
	}

	pipeline(t, input)
}

func Test_parsePostgresConflictsStats(t *testing.T) {
	var testCases = []struct {
		name string
		res  *model.PGResult
		want map[string]postgresConflictStat
	}{
		{
			name: "normal output",
			res: &model.PGResult{
				Nrows: 2,
				Ncols: 6,
				Colnames: []pgconn.FieldDescription{
					{Name: "database"}, {Name: "confl_tablespace"}, {Name: "confl_lock"},
					{Name: "confl_snapshot"}, {Name: "confl_bufferpin"}, {Name: "confl_deadlock"},
				},
				Rows: [][]sql.NullString{
					{
						{String: "testdb1", Valid: true}, {String: "123", Valid: true}, {String: "548", Valid: true},
						{String: "784", Valid: true}, {String: "896", Valid: true}, {String: "896", Valid: true},
					},
					{
						{String: "testdb2", Valid: true}, {}, {}, {}, {}, {},
					},
				},
			},
			want: map[string]postgresConflictStat{
				"testdb1": {database: "testdb1", tablespace: 123, lock: 548, snapshot: 784, bufferpin: 896, deadlock: 896},
				"testdb2": {database: "testdb2"},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := parsePostgresConflictStats(tc.res, []string{"database", "reason"})
			assert.EqualValues(t, tc.want, got)
		})
	}
}
