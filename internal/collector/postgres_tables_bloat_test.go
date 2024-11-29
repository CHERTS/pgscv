package collector

import (
	"database/sql"
	"testing"

	"github.com/cherts/pgscv/internal/model"
	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/assert"
)

func TestPostgresBloatTablesCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"postgres_table_bloat_est_rows",
			"postgres_table_bloat_pct_bloat",
			"postgres_table_bloat_table_bytes",
			"postgres_table_bloat_bloat_bytes",
		},
		collector: NewPostgresBloatTablesCollector,
		service:   model.ServiceTypePostgresql,
	}

	pipeline(t, input)
}

func Test_parsePostgresBloatTableStats(t *testing.T) {
	var testCases = []struct {
		name string
		res  *model.PGResult
		want map[string]postgresBloatTableStat
	}{
		{
			name: "normal output",
			res: &model.PGResult{
				Nrows: 1,
				Ncols: 7,
				Colnames: []pgproto3.FieldDescription{
					{Name: []byte("database")}, {Name: []byte("schema")}, {Name: []byte("table")},
					{Name: []byte("est_rows")}, {Name: []byte("pct_bloat")}, {Name: []byte("bloat_bytes")}, {Name: []byte("table_bytes")},
				},
				Rows: [][]sql.NullString{
					{
						{String: "testdb", Valid: true}, {String: "testschema", Valid: true}, {String: "testrelname", Valid: true},
						{String: "100000", Valid: true}, {String: "55", Valid: true}, {String: "6422528", Valid: true}, {String: "11640832", Valid: true},
					},
				},
			},
			want: map[string]postgresBloatTableStat{
				"testdb/testschema/testrelname": {
					database: "testdb", schema: "testschema", table: "testrelname",
					estrows: 100000, pctbloat: 55, bloatbytes: 6422528, tablebytes: 11640832,
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := parsePostgresBloatTableStats(tc.res, []string{"database", "schema", "table"})
			assert.EqualValues(t, tc.want, got)
		})
	}
}
