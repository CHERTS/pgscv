package collector

import (
	"database/sql"
	"github.com/jackc/pgx/v5/pgconn"
	"testing"

	"github.com/cherts/pgscv/internal/model"
	"github.com/stretchr/testify/assert"
)

func TestPostgresBgwriterCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"postgres_checkpoints_total",
			"postgres_checkpoints_all_total",
			"postgres_checkpoints_seconds_total",
			"postgres_checkpoints_seconds_all_total",
			"postgres_written_bytes_total",
			"postgres_bgwriter_maxwritten_clean_total",
			"postgres_backends_fsync_total",
			"postgres_backends_allocated_bytes_total",
			"postgres_bgwriter_stats_age_seconds_total",
			"postgres_checkpoints_stats_age_seconds_total",
			"postgres_checkpoints_restartpoints_req",
			"postgres_checkpoints_restartpoints_done",
			"postgres_checkpoints_restartpoints_timed",
		},
		collector: NewPostgresBgwriterCollector,
		service:   model.ServiceTypePostgresql,
	}

	pipeline(t, input)
}

func Test_parsePostgresBgwriterStats(t *testing.T) {
	var testCases = []struct {
		name string
		res  *model.PGResult
		want postgresBgwriterStat
	}{
		{
			name: "normal output",
			res: &model.PGResult{
				Nrows: 1,
				Ncols: 15,
				Colnames: []pgconn.FieldDescription{
					{Name: "checkpoints_timed"}, {Name: "checkpoints_req"},
					{Name: "checkpoint_write_time"}, {Name: "checkpoint_sync_time"},
					{Name: "buffers_checkpoint"}, {Name: "buffers_clean"}, {Name: "maxwritten_clean"},
					{Name: "buffers_backend"}, {Name: "buffers_backend_fsync"}, {Name: "buffers_alloc"},
					{Name: "bgwr_stats_age_seconds"}, {Name: "ckpt_stats_age_seconds"}, {Name: "restartpoints_timed"},
					{Name: "restartpoints_req"}, {Name: "restartpoints_done"},
				},
				Rows: [][]sql.NullString{
					{
						{String: "55", Valid: true}, {String: "17", Valid: true},
						{String: "548425", Valid: true}, {String: "5425", Valid: true},
						{String: "5482", Valid: true}, {String: "7584", Valid: true}, {String: "452", Valid: true},
						{String: "6895", Valid: true}, {String: "2", Valid: true}, {String: "48752", Valid: true},
						{String: "5488", Valid: true}, {String: "54388", Valid: true}, {String: "47352", Valid: true},
						{String: "5288", Valid: true}, {String: "1438", Valid: true},
					},
				},
			},
			want: postgresBgwriterStat{
				ckptTimed: 55, ckptReq: 17, ckptWriteTime: 548425, ckptSyncTime: 5425, ckptBuffers: 5482, bgwrBuffers: 7584, bgwrMaxWritten: 452,
				backendBuffers: 6895, backendFsync: 2, backendAllocated: 48752, bgwrStatsAgeSeconds: 5488, ckptStatsAgeSeconds: 54388, ckptRestartpointsTimed: 47352,
				ckptRestartpointsReq: 5288, ckptRestartpointsDone: 1438,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := parsePostgresBgwriterStats(tc.res)
			assert.EqualValues(t, tc.want, got)
		})
	}
}
