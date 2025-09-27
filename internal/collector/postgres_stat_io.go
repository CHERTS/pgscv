package collector

import (
	"context"
	"strconv"
	"strings"
	"sync"

	"github.com/cherts/pgscv/internal/log"
	"github.com/cherts/pgscv/internal/model"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	postgresStatIoQuery17 = "SELECT backend_type, object, context, COALESCE(reads, 0) AS reads, COALESCE(read_time, 0) AS read_time, " +
		"COALESCE(writes, 0) AS writes, COALESCE(write_time, 0) AS write_time, COALESCE(writebacks, 0) AS writebacks, " +
		"COALESCE(writeback_time, 0) AS writeback_time, COALESCE(extends, 0) AS extends, COALESCE(extend_time, 0) AS extend_time, " +
		"COALESCE(hits, 0) AS hits, COALESCE(evictions, 0) AS evictions, COALESCE(reuses, 0) AS reuses, " +
		"COALESCE(fsyncs, 0) AS fsyncs, COALESCE(fsync_time, 0) AS fsync_time, " +
		"COALESCE(reads, 0) * COALESCE(op_bytes, 0) AS read_bytes, " +
		"COALESCE(writes, 0) * COALESCE(op_bytes, 0) AS write_bytes, " +
		"COALESCE(extends, 0) * COALESCE(op_bytes, 0) AS extend_bytes " +
		"FROM pg_stat_io"

	postgresStatIoQueryLatest = "SELECT backend_type, object, context, COALESCE(reads, 0) AS reads, COALESCE(read_time, 0) AS read_time, " +
		"COALESCE(writes, 0) AS writes, COALESCE(write_time, 0) AS write_time, COALESCE(writebacks, 0) AS writebacks, " +
		"COALESCE(writeback_time, 0) AS writeback_time, COALESCE(extends, 0) AS extends, COALESCE(extend_time, 0) AS extend_time, " +
		"COALESCE(hits, 0) AS hits, COALESCE(evictions, 0) AS evictions, COALESCE(reuses, 0) AS reuses, " +
		"COALESCE(fsyncs, 0) AS fsyncs, COALESCE(fsync_time, 0) AS fsync_time, " +
		"COALESCE(read_bytes, 0) AS read_bytes, COALESCE(write_bytes, 0) AS write_bytes, COALESCE(extend_bytes, 0) AS extend_bytes " +
		"FROM pg_stat_io"
)

// postgresStatIOCollector defines metric descriptors and stats store.
type postgresStatIOCollector struct {
	reads         typedDesc
	readTime      typedDesc
	writes        typedDesc
	writeTime     typedDesc
	writebacks    typedDesc
	writebackTime typedDesc
	extends       typedDesc
	extendTime    typedDesc
	hits          typedDesc
	evictions     typedDesc
	reuses        typedDesc
	fsyncs        typedDesc
	fsyncTime     typedDesc
	readBytes     typedDesc
	writeBytes    typedDesc
	extendBytes   typedDesc
	labelNames    []string
}

// NewPostgresStatIOCollector returns a new Collector exposing postgres pg_stat_io stats.
func NewPostgresStatIOCollector(constLabels labels, settings model.CollectorSettings) (Collector, error) {
	var labelNames = []string{"backend_type", "object", "context"}

	return &postgresStatIOCollector{
		labelNames: labelNames,
		reads: newBuiltinTypedDesc(
			descOpts{"postgres", "stat_io", "reads", "Number of read operations, each of the size specified in op_bytes.", 0},
			prometheus.GaugeValue,
			labelNames, constLabels,
			settings.Filters,
		),
		readTime: newBuiltinTypedDesc(
			descOpts{"postgres", "stat_io", "read_time", "Time spent in read operations in milliseconds (if track_io_timing is enabled, otherwise zero)", 0},
			prometheus.GaugeValue,
			labelNames, constLabels,
			settings.Filters,
		),
		writes: newBuiltinTypedDesc(
			descOpts{"postgres", "stat_io", "writes", "Number of write operations, each of the size specified in op_bytes.", 0},
			prometheus.GaugeValue,
			labelNames, constLabels,
			settings.Filters,
		),
		writeTime: newBuiltinTypedDesc(
			descOpts{"postgres", "stat_io", "write_time", "Time spent in write operations in milliseconds (if track_io_timing is enabled, otherwise zero)", 0},
			prometheus.GaugeValue,
			labelNames, constLabels,
			settings.Filters,
		),
		writebacks: newBuiltinTypedDesc(
			descOpts{"postgres", "stat_io", "writebacks", "Number of units of size op_bytes which the process requested the kernel write out to permanent storage.", 0},
			prometheus.GaugeValue,
			labelNames, constLabels,
			settings.Filters,
		),
		writebackTime: newBuiltinTypedDesc(
			descOpts{"postgres", "stat_io", "writeback_time", "Time spent in writeback operations in milliseconds (if track_io_timing is enabled, otherwise zero). ", 0},
			prometheus.GaugeValue,
			labelNames, constLabels,
			settings.Filters,
		),
		extends: newBuiltinTypedDesc(
			descOpts{"postgres", "stat_io", "extends", "Number of relation extend operations, each of the size specified in op_bytes.", 0},
			prometheus.GaugeValue,
			labelNames, constLabels,
			settings.Filters,
		),
		extendTime: newBuiltinTypedDesc(
			descOpts{"postgres", "stat_io", "extend_time", "Time spent in extend operations in milliseconds (if track_io_timing is enabled, otherwise zero)", 0},
			prometheus.GaugeValue,
			labelNames, constLabels,
			settings.Filters,
		),
		hits: newBuiltinTypedDesc(
			descOpts{"postgres", "stat_io", "hits", "The number of times a desired block was found in a shared buffer.", 0},
			prometheus.GaugeValue,
			labelNames, constLabels,
			settings.Filters,
		),
		evictions: newBuiltinTypedDesc(
			descOpts{"postgres", "stat_io", "evictions", "Number of times a block has been written out from a shared or local buffer in order to make it available for another use.", 0},
			prometheus.GaugeValue,
			labelNames, constLabels,
			settings.Filters,
		),
		reuses: newBuiltinTypedDesc(
			descOpts{"postgres", "stat_io", "reuses", "The number of times an existing buffer in a size-limited ring buffer outside of shared buffers was reused as part of an I/O operation in the bulkread, bulkwrite, or vacuum contexts.", 0},
			prometheus.GaugeValue,
			labelNames, constLabels,
			settings.Filters,
		),
		fsyncs: newBuiltinTypedDesc(
			descOpts{"postgres", "stat_io", "fsyncs", "Number of fsync calls. These are only tracked in context normal.", 0},
			prometheus.GaugeValue,
			labelNames, constLabels,
			settings.Filters,
		),
		fsyncTime: newBuiltinTypedDesc(
			descOpts{"postgres", "stat_io", "fsync_time", "Time spent in fsync operations in milliseconds (if track_io_timing is enabled, otherwise zero)", 0},
			prometheus.GaugeValue,
			labelNames, constLabels,
			settings.Filters,
		),
		readBytes: newBuiltinTypedDesc(
			descOpts{"postgres", "stat_io", "read_bytes", "Number of read, in bytes.", 0},
			prometheus.GaugeValue,
			labelNames, constLabels,
			settings.Filters,
		),
		writeBytes: newBuiltinTypedDesc(
			descOpts{"postgres", "stat_io", "write_bytes", "Number of write, in bytes.", 0},
			prometheus.GaugeValue,
			labelNames, constLabels,
			settings.Filters,
		),
		extendBytes: newBuiltinTypedDesc(
			descOpts{"postgres", "stat_io", "extend_bytes", "Number of relation extend, in bytes.", 0},
			prometheus.GaugeValue,
			labelNames, constLabels,
			settings.Filters,
		),
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *postgresStatIOCollector) Update(ctx context.Context, config Config, ch chan<- prometheus.Metric) error {
	if config.pgVersion.Numeric < PostgresV16 {
		log.Debugln("[postgres stat_io collector]: pg_stat_io view are not available, required Postgres 16 or newer")
		return nil
	}

	conn := config.DB
	query := selectStatIOQuery(config.pgVersion.Numeric)
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	var err error

	cacheKey, res, _ := getFromCache(config.CacheConfig, config.ConnString, collectorPostgresStatIO, query)
	if res == nil {
		res, err = conn.Query(ctx, query)
		if err != nil {
			log.Warnf("get pg_stat_io failed: %s; skip", err)
			return err
		}
		saveToCache(collectorPostgresStatIO, wg, config.CacheConfig, cacheKey, res)
	}

	stats := parsePostgresStatIO(res, c.labelNames)

	for _, stat := range stats {
		ch <- c.reads.newConstMetric(stat.Reads, stat.BackendType, stat.IoObject, stat.IoContext)
		ch <- c.readTime.newConstMetric(stat.ReadTime, stat.BackendType, stat.IoObject, stat.IoContext)
		ch <- c.writes.newConstMetric(stat.Writes, stat.BackendType, stat.IoObject, stat.IoContext)
		ch <- c.writeTime.newConstMetric(stat.WriteTime, stat.BackendType, stat.IoObject, stat.IoContext)
		ch <- c.writebacks.newConstMetric(stat.Writebacks, stat.BackendType, stat.IoObject, stat.IoContext)
		ch <- c.writebackTime.newConstMetric(stat.WritebackTime, stat.BackendType, stat.IoObject, stat.IoContext)
		ch <- c.extends.newConstMetric(stat.Extends, stat.BackendType, stat.IoObject, stat.IoContext)
		ch <- c.extendTime.newConstMetric(stat.ExtendTime, stat.BackendType, stat.IoObject, stat.IoContext)
		ch <- c.hits.newConstMetric(stat.Hits, stat.BackendType, stat.IoObject, stat.IoContext)
		ch <- c.evictions.newConstMetric(stat.Evictions, stat.BackendType, stat.IoObject, stat.IoContext)
		ch <- c.reuses.newConstMetric(stat.Reuses, stat.BackendType, stat.IoObject, stat.IoContext)
		ch <- c.fsyncs.newConstMetric(stat.Fsyncs, stat.BackendType, stat.IoObject, stat.IoContext)
		ch <- c.fsyncTime.newConstMetric(stat.FsyncTime, stat.BackendType, stat.IoObject, stat.IoContext)
		ch <- c.readBytes.newConstMetric(stat.ReadBytes, stat.BackendType, stat.IoObject, stat.IoContext)
		ch <- c.writeBytes.newConstMetric(stat.WriteBytes, stat.BackendType, stat.IoObject, stat.IoContext)
		ch <- c.extendBytes.newConstMetric(stat.ExtendBytes, stat.BackendType, stat.IoObject, stat.IoContext)
	}

	return nil
}

// postgresStatIO
type postgresStatIO struct {
	BackendType   string // a backend type like "autovacuum worker"
	IoObject      string // "relation" or "temp relation"
	IoContext     string // "normal", "vacuum", "bulkread" or "bulkwrite"
	Reads         float64
	ReadTime      float64
	Writes        float64
	WriteTime     float64
	Writebacks    float64
	WritebackTime float64
	Extends       float64
	ExtendTime    float64
	Hits          float64
	Evictions     float64
	Reuses        float64
	Fsyncs        float64
	FsyncTime     float64
	ReadBytes     float64
	WriteBytes    float64
	ExtendBytes   float64
}

// parsePostgresStatIO parses PGResult and returns structs with stats values.
func parsePostgresStatIO(r *model.PGResult, labelNames []string) map[string]postgresStatIO {
	log.Debug("parse postgres stat_io stats")

	var stats = make(map[string]postgresStatIO)

	for _, row := range r.Rows {
		var BackendType, IoObject, IoContext string

		for i, colname := range r.Colnames {
			switch string(colname.Name) {
			case "backend_type":
				BackendType = row[i].String
			case "object":
				IoObject = row[i].String
			case "context":
				IoContext = row[i].String
			}
		}

		// create a stat_io name consisting of trio BackendType/IoObject/IoContext
		statIo := strings.Join([]string{BackendType, IoObject, IoContext}, "/")

		// Put stats with labels (but with no data values yet) into stats store.
		if _, ok := stats[statIo]; !ok {
			stats[statIo] = postgresStatIO{BackendType: BackendType, IoObject: IoObject, IoContext: IoContext}
		}

		for i, colname := range r.Colnames {
			// skip columns if its value used as a label
			if stringsContains(labelNames, string(colname.Name)) {
				continue
			}

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

			s := stats[statIo]

			switch string(colname.Name) {
			case "reads":
				s.Reads = v
			case "read_time":
				s.ReadTime = v
			case "writes":
				s.Writes = v
			case "write_time":
				s.WriteTime = v
			case "writebacks":
				s.Writebacks = v
			case "writeback_time":
				s.WritebackTime = v
			case "extends":
				s.Extends = v
			case "extend_time":
				s.ExtendTime = v
			case "hits":
				s.Hits = v
			case "evictions":
				s.Evictions = v
			case "fsyncs":
				s.Fsyncs = v
			case "fsync_time":
				s.FsyncTime = v
			case "read_bytes":
				s.ReadBytes = v
			case "write_bytes":
				s.WriteBytes = v
			case "extend_bytes":
				s.ExtendBytes = v
			default:
				continue
			}

			stats[statIo] = s
		}
	}

	return stats
}

// selectStatIOQuery returns suitable stat_io query depending on passed version.
func selectStatIOQuery(version int) string {
	switch {
	case version < PostgresV18:
		return postgresStatIoQuery17
	default:
		return postgresStatIoQueryLatest
	}
}
