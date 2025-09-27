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

const walArchivingQuery = "SELECT archived_count, failed_count, " +
	"EXTRACT(EPOCH FROM now() - last_archived_time) AS since_last_archive_seconds, " +
	"(SELECT count(*) FROM pg_ls_archive_statusdir() WHERE name ~'.ready') AS lag_files " +
	"FROM pg_stat_archiver WHERE archived_count > 0"

type postgresWalArchivingCollector struct {
	archived             typedDesc
	failed               typedDesc
	sinceArchivedSeconds typedDesc
	archivingLag         typedDesc
}

// NewPostgresWalArchivingCollector returns a new Collector exposing postgres WAL archiving stats.
// For details see https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-ARCHIVER-VIEW
func NewPostgresWalArchivingCollector(constLabels labels, settings model.CollectorSettings) (Collector, error) {
	return &postgresWalArchivingCollector{
		archived: newBuiltinTypedDesc(
			descOpts{"postgres", "archiver", "archived_total", "Total number of WAL segments had been successfully archived.", 0},
			prometheus.CounterValue,
			nil, constLabels,
			settings.Filters,
		),
		failed: newBuiltinTypedDesc(
			descOpts{"postgres", "archiver", "failed_total", "Total number of attempts when WAL segments had been failed to archive.", 0},
			prometheus.CounterValue,
			nil, constLabels,
			settings.Filters,
		),
		sinceArchivedSeconds: newBuiltinTypedDesc(
			descOpts{"postgres", "archiver", "since_last_archive_seconds", "Number of seconds since last WAL segment had been successfully archived.", 0},
			prometheus.GaugeValue,
			nil, constLabels,
			settings.Filters,
		),
		archivingLag: newBuiltinTypedDesc(
			descOpts{"postgres", "archiver", "lag_bytes", "Amount of WAL segments ready, but not archived, in bytes.", 0},
			prometheus.GaugeValue,
			nil, constLabels,
			settings.Filters,
		),
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *postgresWalArchivingCollector) Update(ctx context.Context, config Config, ch chan<- prometheus.Metric) error {

	if config.pgVersion.Numeric < PostgresV12 {
		log.Debugln("[postgres WAL archiver collector]: some system functions are not available, required Postgres 12 or newer")
		return nil
	}
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	var err error
	cacheKey, res, _ := getFromCache(config.CacheConfig, config.ConnString, collectorPostgresArchiver, walArchivingQuery)
	if res == nil {
		res, err = config.DB.Query(ctx, walArchivingQuery)
		if err != nil {
			return err
		}
		saveToCache(collectorPostgresArchiver, wg, config.CacheConfig, cacheKey, res)
	}

	stats := parsePostgresWalArchivingStats(res)

	if stats.archived == 0 {
		log.Debugln("zero archived WAL segments, skip collecting archiver stats")
		return nil
	}

	ch <- c.archived.newConstMetric(stats.archived)
	ch <- c.failed.newConstMetric(stats.failed)
	ch <- c.sinceArchivedSeconds.newConstMetric(stats.sinceArchivedSeconds)
	ch <- c.archivingLag.newConstMetric(stats.lagFiles * float64(config.walSegmentSize))

	return nil
}

// postgresWalArchivingStat describes stats about WAL archiving.
type postgresWalArchivingStat struct {
	archived             float64
	failed               float64
	sinceArchivedSeconds float64
	lagFiles             float64
}

// parsePostgresWalArchivingStats parses PGResult, extract data and return struct with stats values.
func parsePostgresWalArchivingStats(r *model.PGResult) postgresWalArchivingStat {
	log.Debug("parse postgres WAL archiving stats")

	var stats postgresWalArchivingStat

	// process row by row
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
			case "archived_count":
				stats.archived = v
			case "failed_count":
				stats.failed = v
			case "since_last_archive_seconds":
				stats.sinceArchivedSeconds = v
			case "lag_files":
				stats.lagFiles = v
			default:
				continue
			}
		}
	}

	return stats
}
