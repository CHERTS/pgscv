package collector

import (
	"database/sql"
	"github.com/jackc/pgx/v5/pgconn"
	"testing"

	"github.com/cherts/pgscv/internal/model"
	"github.com/stretchr/testify/assert"
)

func TestPostgresStatSubscriptionCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{},
		optional: []string{
			"postgres_stat_subscription_received_lsn",
			"postgres_stat_subscription_reported_lsn",
			"postgres_stat_subscription_msg_send_time",
			"postgres_stat_subscription_msg_recv_time",
			"postgres_stat_subscription_reported_time",
			"postgres_stat_subscription_error_count",
			"postgres_stat_subscription_confl_count",
		},
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
				Ncols: 10,
				Colnames: []pgconn.FieldDescription{
					{Name: "subid"}, {Name: "subname"}, {Name: "pid"},
					{Name: "worker_type"}, {Name: "received_lsn"}, {Name: "reported_lsn"},
					{Name: "msg_send_time"}, {Name: "msg_recv_time"}, {Name: "reported_time"},
					{Name: "apply_error_count"}, {Name: "sync_error_count"},
				},
				Rows: [][]sql.NullString{
					{
						{String: "123456", Valid: true}, {String: "test_sub1", Valid: true}, {String: "123", Valid: true},
						{String: "apply", Valid: true}, {String: "43245505613688", Valid: true}, {String: "43245505613688", Valid: true},
						{String: "1749455313.132133", Valid: true}, {String: "1749455313.132133", Valid: true}, {String: "1749455313.132133", Valid: true},
						{String: "0", Valid: true}, {String: "0", Valid: true},
					},
					{
						{String: "654321", Valid: true}, {String: "test_sub2", Valid: true}, {String: "321", Valid: true},
						{String: "table synchronization", Valid: true}, {String: "43245505613688", Valid: true}, {String: "43245505613688", Valid: true},
						{String: "1749455313.132133", Valid: true}, {String: "1749455313.132133", Valid: true}, {String: "1749455313.132133", Valid: true},
						{String: "0", Valid: true}, {String: "0", Valid: true},
					},
				},
			},
			want: map[string]postgresSubscriptionStat{
				"123": {
					SubID: "123456", SubName: "test_sub1", Pid: "123", WorkerType: "apply",
					values: map[string]float64{
						"received_lsn": 43245505613688, "reported_lsn": 43245505613688,
						"msg_send_time": 1749455313.132133, "msg_recv_time": 1749455313.132133, "reported_time": 1749455313.132133,
						"apply_error_count": 0, "sync_error_count": 0,
					},
				},
				"321": {
					SubID: "654321", SubName: "test_sub2", Pid: "321", WorkerType: "table synchronization",
					values: map[string]float64{
						"received_lsn": 43245505613688, "reported_lsn": 43245505613688,
						"msg_send_time": 1749455313.132133, "msg_recv_time": 1749455313.132133, "reported_time": 1749455313.132133,
						"apply_error_count": 0, "sync_error_count": 0,
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := parsePostgresSubscriptionStat(tc.res, []string{"subid", "subname", "worker_type", "type"})
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
		{version: 170000, want: postgresStatSubscriptionQuery17},
		{version: 180000, want: postgresStatSubscriptionQueryLatest},
	}

	for _, tc := range testcases {
		t.Run("", func(t *testing.T) {
			assert.Equal(t, tc.want, selectSubscriptionQuery(tc.version))
		})
	}
}
