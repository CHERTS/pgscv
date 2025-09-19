package collector

import (
	"database/sql"
	"github.com/jackc/pgx/v5/pgconn"
	"testing"

	"github.com/cherts/pgscv/internal/model"
	"github.com/stretchr/testify/assert"
)

func TestPostgresStatSlruCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"postgres_stat_slru_blks_zeroed",
			"postgres_stat_slru_blks_hit",
			"postgres_stat_slru_blks_read",
			"postgres_stat_slru_blks_written",
			"postgres_stat_slru_blks_exists",
			"postgres_stat_slru_flushes",
			"postgres_stat_slru_truncates",
		},
		collector: NewPostgresStatSlruCollector,
		service:   model.ServiceTypePostgresql,
	}

	pipeline(t, input)
}

func Test_parsePostgresStatSlru(t *testing.T) {
	var testCases = []struct {
		name string
		res  *model.PGResult
		want map[string]postgresStatSlru
	}{
		{
			name: "normal output, Postgres 13",
			res: &model.PGResult{
				Nrows: 1,
				Ncols: 8,
				Colnames: []pgconn.FieldDescription{
					{Name: "name"}, {Name: "blks_zeroed"}, {Name: "blks_hit"}, {Name: "blks_read"},
					{Name: "blks_written"}, {Name: "blks_exists"}, {Name: "flushes"}, {Name: "truncates"},
				},
				Rows: [][]sql.NullString{
					{
						{String: "subtransaction", Valid: true}, {String: "2972", Valid: true}, {String: "0", Valid: true}, {String: "0", Valid: true},
						{String: "2867", Valid: true}, {String: "0", Valid: true}, {String: "527", Valid: true}, {String: "527", Valid: true},
					},
				},
			},
			want: map[string]postgresStatSlru{
				"subtransaction": {
					SlruName: "subtransaction", BlksZeroed: 2972, BlksHit: 0, BlksRead: 0,
					BlksWritten: 2867, BlksExists: 0, Flushes: 527, Truncates: 527,
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := parsePostgresStatSlru(tc.res, []string{"name"})
			assert.EqualValues(t, tc.want, got)
		})
	}
}
