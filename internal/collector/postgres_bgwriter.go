// Package collector is a pgSCV collectors
package collector

import (
	"context"
	"strconv"
	"sync"

	"github.com/cherts/pgscv/internal/log"
	"github.com/cherts/pgscv/internal/model"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	postgresBgwriterQuery16 = "SELECT " +
		"checkpoints_timed, checkpoints_req, checkpoint_write_time, checkpoint_sync_time, " +
		"buffers_checkpoint, buffers_clean, maxwritten_clean, " +
		"buffers_backend, buffers_backend_fsync, buffers_alloc, " +
		"COALESCE(EXTRACT(EPOCH FROM AGE(now(), stats_reset)), 0) as bgwr_stats_age_seconds " +
		"FROM pg_stat_bgwriter"

	postgresBgwriterQuery17 = "WITH ckpt AS (" +
		"SELECT num_timed AS checkpoints_timed, num_requested AS checkpoints_req, restartpoints_timed, restartpoints_req, " +
		"restartpoints_done, write_time AS checkpoint_write_time, sync_time AS checkpoint_sync_time, buffers_written AS buffers_checkpoint, " +
		"COALESCE(EXTRACT(EPOCH FROM AGE(now(), stats_reset)), 0) as ckpt_stats_age_seconds FROM pg_stat_checkpointer), " +
		"bgwr AS (" +
		"SELECT buffers_clean, maxwritten_clean, buffers_alloc, " +
		"COALESCE(EXTRACT(EPOCH FROM age(now(), stats_reset)), 0) as bgwr_stats_age_seconds FROM pg_stat_bgwriter), " +
		"stat_io AS (" +
		"SELECT SUM(writes) AS buffers_backend, SUM(fsyncs) AS buffers_backend_fsync FROM pg_stat_io WHERE backend_type='background writer') " +
		"SELECT ckpt.*, bgwr.*, stat_io.* FROM ckpt, bgwr, stat_io"

	postgresBgwriterQueryLatest = "WITH ckpt AS (" +
		"SELECT num_timed AS checkpoints_timed, num_requested AS checkpoints_req, num_done AS checkpoints_done, " +
		"restartpoints_timed, restartpoints_req, restartpoints_done, write_time AS checkpoint_write_time, " +
		"sync_time AS checkpoint_sync_time, buffers_written AS buffers_checkpoint, slru_written AS buffers_slru, " +
		"COALESCE(EXTRACT(EPOCH FROM AGE(now(), stats_reset)), 0) as ckpt_stats_age_seconds FROM pg_stat_checkpointer), " +
		"bgwr AS (" +
		"SELECT buffers_clean, maxwritten_clean, buffers_alloc, " +
		"COALESCE(EXTRACT(EPOCH FROM age(now(), stats_reset)), 0) as bgwr_stats_age_seconds FROM pg_stat_bgwriter), " +
		"stat_io AS (" +
		"SELECT SUM(writes) AS buffers_backend, SUM(fsyncs) AS buffers_backend_fsync FROM pg_stat_io WHERE backend_type='background writer') " +
		"SELECT ckpt.*, bgwr.*, stat_io.* FROM ckpt, bgwr, stat_io"
)

type postgresBgwriterCollector struct {
	descs map[string]typedDesc
}

// NewPostgresBgwriterCollector returns a new Collector exposing postgres bgwriter and checkpointer stats.
// For details see https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STAT-BGWRITER-VIEW
func NewPostgresBgwriterCollector(constLabels labels, settings model.CollectorSettings) (Collector, error) {
	return &postgresBgwriterCollector{
		descs: map[string]typedDesc{
			"checkpoints": newBuiltinTypedDesc(
				descOpts{"postgres", "checkpoints", "total", "Total number of checkpoints that have been performed of each type.", 0},
				prometheus.CounterValue,
				[]string{"checkpoint"}, constLabels,
				settings.Filters,
			),
			"checkpoints_all": newBuiltinTypedDesc(
				descOpts{"postgres", "checkpoints", "all_total", "Total number of checkpoints that have been performed.", 0},
				prometheus.CounterValue,
				nil, constLabels,
				settings.Filters,
			),
			"checkpoint_time": newBuiltinTypedDesc(
				descOpts{"postgres", "checkpoints", "seconds_total", "Total amount of time that has been spent processing data during checkpoint in each stage, in seconds.", .001},
				prometheus.CounterValue,
				[]string{"stage"}, constLabels,
				settings.Filters,
			),
			"checkpoint_time_all": newBuiltinTypedDesc(
				descOpts{"postgres", "checkpoints", "seconds_all_total", "Total amount of time that has been spent processing data during checkpoint, in seconds.", .001},
				prometheus.CounterValue,
				nil, constLabels,
				settings.Filters,
			),
			"written_bytes": newBuiltinTypedDesc(
				descOpts{"postgres", "written", "bytes_total", "Total number of bytes written by each subsystem, in bytes.", 0},
				prometheus.CounterValue,
				[]string{"process"}, constLabels,
				settings.Filters,
			),
			"maxwritten_clean": newBuiltinTypedDesc(
				descOpts{"postgres", "bgwriter", "maxwritten_clean_total", "Total number of times the background writer stopped a cleaning scan because it had written too many buffers.", 0},
				prometheus.CounterValue,
				nil, constLabels,
				settings.Filters,
			),
			"buffers_backend_fsync": newBuiltinTypedDesc(
				descOpts{"postgres", "backends", "fsync_total", "Total number of times a backends had to execute its own fsync() call.", 0},
				prometheus.CounterValue,
				nil, constLabels,
				settings.Filters,
			),
			"alloc_bytes": newBuiltinTypedDesc(
				descOpts{"postgres", "backends", "allocated_bytes_total", "Total number of bytes allocated by backends.", 0},
				prometheus.CounterValue,
				nil, constLabels,
				settings.Filters,
			),
			"bgwr_stats_age_seconds": newBuiltinTypedDesc(
				descOpts{"postgres", "bgwriter", "stats_age_seconds_total", "The age of the background writer activity statistics, in seconds.", 0},
				prometheus.CounterValue,
				nil, constLabels,
				settings.Filters,
			),
			"ckpt_stats_age_seconds": newBuiltinTypedDesc(
				descOpts{"postgres", "checkpoints", "stats_age_seconds_total", "The age of the checkpointer activity statistics, in seconds (since v17).", 0},
				prometheus.CounterValue,
				nil, constLabels,
				settings.Filters,
			),
			"checkpoint_restartpointstimed": newBuiltinTypedDesc(
				descOpts{"postgres", "checkpoints", "restartpoints_timed", "Number of scheduled restartpoints due to timeout or after a failed attempt to perform it (since v17).", 0},
				prometheus.CounterValue,
				nil, constLabels,
				settings.Filters,
			),
			"checkpoint_restartpointsreq": newBuiltinTypedDesc(
				descOpts{"postgres", "checkpoints", "restartpoints_req", "Number of requested restartpoints (since v17).", 0},
				prometheus.CounterValue,
				nil, constLabels,
				settings.Filters,
			),
			"checkpoint_restartpointsdone": newBuiltinTypedDesc(
				descOpts{"postgres", "checkpoints", "restartpoints_done", "Number of restartpoints that have been performed (since v17).", 0},
				prometheus.CounterValue,
				nil, constLabels,
				settings.Filters,
			),
		},
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *postgresBgwriterCollector) Update(ctx context.Context, config Config, ch chan<- prometheus.Metric) error {
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	var err error

	query := selectBgwriterQuery(config.pgVersion.Numeric)
	cacheKey, res, _ := getFromCache(config.CacheConfig, config.ConnString, collectorPostgresBgWriter, query)
	if res == nil {
		res, err = config.DB.Query(ctx, query)
		if err != nil {
			return err
		}
		saveToCache(collectorPostgresBgWriter, wg, config.CacheConfig, cacheKey, res)
	}

	stats := parsePostgresBgwriterStats(res)
	blockSize := float64(config.blockSize)

	for name, desc := range c.descs {
		switch name {
		case "checkpoints":
			ch <- desc.newConstMetric(stats.ckptTimed, "timed")
			ch <- desc.newConstMetric(stats.ckptReq, "req")
			if config.pgVersion.Numeric >= PostgresV18 {
				ch <- desc.newConstMetric(stats.ckptDone, "done")
			}
		case "checkpoints_all":
			ch <- desc.newConstMetric(stats.ckptTimed + stats.ckptReq)
		case "checkpoint_time":
			ch <- desc.newConstMetric(stats.ckptWriteTime, "write")
			ch <- desc.newConstMetric(stats.ckptSyncTime, "sync")
		case "checkpoint_time_all":
			ch <- desc.newConstMetric(stats.ckptWriteTime + stats.ckptSyncTime)
		case "maxwritten_clean":
			ch <- desc.newConstMetric(stats.bgwrMaxWritten)
		case "written_bytes":
			ch <- desc.newConstMetric(stats.ckptBuffers*blockSize, "checkpointer")
			ch <- desc.newConstMetric(stats.bgwrBuffers*blockSize, "bgwriter")
			ch <- desc.newConstMetric(stats.backendBuffers*blockSize, "backend")
			if config.pgVersion.Numeric >= PostgresV18 {
				ch <- desc.newConstMetric(stats.slruBuffers*blockSize, "slru")
			}
		case "buffers_backend_fsync":
			ch <- desc.newConstMetric(stats.backendFsync)
		case "alloc_bytes":
			ch <- desc.newConstMetric(stats.backendAllocated * blockSize)
		case "bgwr_stats_age_seconds":
			ch <- desc.newConstMetric(stats.bgwrStatsAgeSeconds)
		case "ckpt_stats_age_seconds":
			ch <- desc.newConstMetric(stats.ckptStatsAgeSeconds)
		case "checkpoint_restartpointstimed":
			ch <- desc.newConstMetric(stats.ckptRestartpointsTimed)
		case "checkpoint_restartpointsreq":
			ch <- desc.newConstMetric(stats.ckptRestartpointsReq)
		case "checkpoint_restartpointsdone":
			ch <- desc.newConstMetric(stats.ckptRestartpointsDone)
		default:
			log.Debugf("unknown desc name: %s, skip", name)
			continue
		}
	}

	return nil
}

// postgresBgwriterStat describes stats related to Postgres background writes.
type postgresBgwriterStat struct {
	ckptTimed              float64
	ckptReq                float64
	ckptDone               float64
	ckptWriteTime          float64
	ckptSyncTime           float64
	ckptBuffers            float64
	ckptRestartpointsTimed float64
	ckptRestartpointsReq   float64
	ckptRestartpointsDone  float64
	ckptStatsAgeSeconds    float64
	bgwrBuffers            float64
	bgwrMaxWritten         float64
	backendBuffers         float64
	slruBuffers            float64
	backendFsync           float64
	backendAllocated       float64
	bgwrStatsAgeSeconds    float64
}

// parsePostgresBgwriterStats parses PGResult and returns struct with data values
func parsePostgresBgwriterStats(r *model.PGResult) postgresBgwriterStat {
	log.Debug("parse postgres bgwriter/checkpointer stats")

	var stats postgresBgwriterStat

	for _, row := range r.Rows {
		for i, colname := range r.Colnames {
			// Skip empty (NULL) values.
			if !row[i].Valid {
				continue
			}

			// Get data value and convert it to float64 used by Prometheus.
			v, err := strconv.ParseFloat(row[i].String, 64)
			if err != nil {
				log.Errorf("invalid input, parse '%s' failed: %s; skip", row[i].String, err)
				continue
			}

			// Update stats struct
			switch string(colname.Name) {
			case "checkpoints_timed":
				stats.ckptTimed = v
			case "checkpoints_req":
				stats.ckptReq = v
			case "checkpoints_done":
				stats.ckptDone = v
			case "checkpoint_write_time":
				stats.ckptWriteTime = v
			case "checkpoint_sync_time":
				stats.ckptSyncTime = v
			case "buffers_checkpoint":
				stats.ckptBuffers = v
			case "buffers_clean":
				stats.bgwrBuffers = v
			case "maxwritten_clean":
				stats.bgwrMaxWritten = v
			case "buffers_backend":
				stats.backendBuffers = v
			case "buffers_slru":
				stats.slruBuffers = v
			case "buffers_backend_fsync":
				stats.backendFsync = v
			case "buffers_alloc":
				stats.backendAllocated = v
			case "bgwr_stats_age_seconds":
				stats.bgwrStatsAgeSeconds = v
			case "ckpt_stats_age_seconds":
				stats.ckptStatsAgeSeconds = v
			case "restartpoints_timed":
				stats.ckptRestartpointsTimed = v
			case "restartpoints_req":
				stats.ckptRestartpointsReq = v
			case "restartpoints_done":
				stats.ckptRestartpointsDone = v
			default:
				continue
			}
		}
	}

	return stats
}

// selectBgwriterQuery returns suitable bgwriter/checkpointer query depending on passed version.
func selectBgwriterQuery(version int) string {
	switch {
	case version < PostgresV17:
		return postgresBgwriterQuery16
	case version < PostgresV18:
		return postgresBgwriterQuery17
	default:
		return postgresBgwriterQueryLatest
	}
}
