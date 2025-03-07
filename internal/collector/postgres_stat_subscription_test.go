package collector

import (
	"database/sql"
	"testing"

	"github.com/cherts/pgscv/internal/model"
	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/assert"
)

func TestPostgresStatSubscriptionCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"postgres_stat_subscription_lag_bytes",
			"postgres_stat_subscription_error_count",
		},
		optional:  []string{},
		collector: NewPostgresStatSubscriptionCollector,
		service:   model.ServiceTypePostgresql,
	}

	pipeline(t, input)
}

func Test_parsePostgresSubscriptionStat(t *testing.T) {
	var testCases = []struct {
		name string
		res  *model.PGResult
		want map[string]postgresSubscriptionStat
	}{
		{
			name: "normal output",
			res: &model.PGResult{
				Nrows: 1,
				Ncols: 8,
				Colnames: []pgproto3.FieldDescription{
					{Name: []byte("pid")}, {Name: []byte("subname")}, {Name: []byte("relname")}, {Name: []byte("worker_type")},
					{Name: []byte("received")}, {Name: []byte("latest_end")},
					{Name: []byte("apply_error_count")}, {Name: []byte("sync_error_count")},
				},
				Rows: [][]sql.NullString{
					{
						{String: "123456", Valid: true}, {String: "test_sub1", Valid: true}, {String: "test_table_1", Valid: true}, {String: "parallel apply", Valid: true},
						{String: "100", Valid: true}, {String: "200", Valid: true},
						{String: "1", Valid: true}, {String: "2", Valid: true},
					},
					{
						{String: "654321", Valid: true}, {String: "test_sub2", Valid: true}, {String: "test_table_2", Valid: true}, {String: "table synchronization", Valid: true},
						{String: "100", Valid: true}, {String: "200", Valid: true},
						{String: "1", Valid: true}, {String: "2", Valid: true},
					},
				},
			},
			want: map[string]postgresSubscriptionStat{
				"123456": {
					Pid: "123456", SubName: "test_sub1", RelName: "test_table_1", WorkerType: "parallel apply",
					values: map[string]float64{
						"received": 100, "latest_end": 200, "apply_error_count": 1, "sync_error_count": 2,
					},
				},
				"654321": {
					Pid: "654321", SubName: "test_sub2", RelName: "test_table_2", WorkerType: "table synchronization",
					values: map[string]float64{
						"received": 100, "latest_end": 200, "apply_error_count": 1, "sync_error_count": 2,
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := parsePostgresSubscriptionStat(tc.res, []string{"subname", "relname", "worker_type"})
			assert.EqualValues(t, tc.want, got)
		})
	}
}

func Test_selectSubscriptionQuery(t *testing.T) {
	var testcases = []struct {
		version int
		want    string
	}{
		{version: 100000, want: postgresStatSubscriptionQuery14},
		{version: 120000, want: postgresStatSubscriptionQuery14},
		{version: 130000, want: postgresStatSubscriptionQuery14},
		{version: 150000, want: postgresStatSubscriptionQuery16},
		{version: 160000, want: postgresStatSubscriptionQuery16},
		{version: 170000, want: postgresStatSubscriptionQueryLatest},
		{version: 180000, want: postgresStatSubscriptionQueryLatest},
	}

	for _, tc := range testcases {
		t.Run("", func(t *testing.T) {
			assert.Equal(t, tc.want, selectSubscriptionQuery(tc.version))
		})
	}
}
