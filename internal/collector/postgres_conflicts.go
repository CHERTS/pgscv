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
	postgresDatabaseConflictsQuery15 = "SELECT datname AS database, confl_tablespace, confl_lock, confl_snapshot, confl_bufferpin, confl_deadlock " +
		"FROM pg_stat_database_conflicts WHERE pg_is_in_recovery() = 't'"

	postgresDatabaseConflictsQueryLatest = "SELECT datname AS database, confl_tablespace, confl_lock, confl_snapshot, confl_bufferpin, confl_deadlock, confl_active_logicalslot " +
		"FROM pg_stat_database_conflicts WHERE pg_is_in_recovery() = 't'"
)

type postgresConflictsCollector struct {
	conflicts typedDesc
}

// NewPostgresConflictsCollector returns a new Collector exposing postgres databases recovery conflicts stats.
// For details see https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STAT-DATABASE-CONFLICTS-VIEW
func NewPostgresConflictsCollector(constLabels labels, settings model.CollectorSettings) (Collector, error) {
	return &postgresConflictsCollector{
		conflicts: newBuiltinTypedDesc(
			descOpts{"postgres", "recovery", "conflicts_total", "Total number of recovery conflicts occurred by each conflict type.", 0},
			prometheus.CounterValue,
			[]string{"database", "conflict"}, constLabels,
			settings.Filters,
		),
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *postgresConflictsCollector) Update(ctx context.Context, config Config, ch chan<- prometheus.Metric) error {
	conn := config.DB
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	var err error

	query := selectDatabaseConflictsQuery(config.pgVersion.Numeric)
	cacheKey, res, _ := getFromCache(config.CacheConfig, config.ConnString, collectorPostgresConflicts, query)
	if res == nil {
		res, err = conn.Query(ctx, query)
		if err != nil {
			return err
		}
		saveToCache(collectorPostgresConflicts, wg, config.CacheConfig, cacheKey, res)
	}

	stats := parsePostgresConflictStats(res, c.conflicts.labelNames)

	for _, stat := range stats {
		ch <- c.conflicts.newConstMetric(stat.tablespace, stat.database, "tablespace")
		ch <- c.conflicts.newConstMetric(stat.lock, stat.database, "lock")
		ch <- c.conflicts.newConstMetric(stat.snapshot, stat.database, "snapshot")
		ch <- c.conflicts.newConstMetric(stat.bufferpin, stat.database, "bufferpin")
		ch <- c.conflicts.newConstMetric(stat.deadlock, stat.database, "deadlock")
		ch <- c.conflicts.newConstMetric(stat.activeLogicalslot, stat.database, "active_logicalslot")
	}

	return nil
}

// postgresConflictStat represents per-database recovery conflicts stats based on pg_stat_database_conflicts.
type postgresConflictStat struct {
	database          string
	tablespace        float64
	lock              float64
	snapshot          float64
	bufferpin         float64
	deadlock          float64
	activeLogicalslot float64
}

// parsePostgresDatabasesStats parses PGResult, extract data and return struct with stats values.
func parsePostgresConflictStats(r *model.PGResult, labelNames []string) map[string]postgresConflictStat {
	log.Debug("parse postgres database conflicts stats")

	var stats = make(map[string]postgresConflictStat)

	// process row by row
	for _, row := range r.Rows {
		stat := postgresConflictStat{}

		// collect label values
		for i, colname := range r.Colnames {
			switch string(colname.Name) {
			case "database":
				stat.database = row[i].String
			}
		}

		// Define a map key as a database name.
		databaseFQName := stat.database

		// Put stats with labels (but with no data values yet) into stats store.
		stats[databaseFQName] = stat

		// fetch data values from columns
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

			s := stats[databaseFQName]

			// Run column-specific logic
			switch string(colname.Name) {
			case "confl_tablespace":
				s.tablespace = v
			case "confl_lock":
				s.lock = v
			case "confl_snapshot":
				s.snapshot = v
			case "confl_bufferpin":
				s.bufferpin = v
			case "confl_deadlock":
				s.deadlock = v
			case "confl_active_logicalslot":
				s.activeLogicalslot = v
			default:
				continue
			}

			stats[databaseFQName] = s
		}
	}

	return stats
}

// selectDatabaseConflictsQuery returns suitable pg_stat_database_conflicts query depending on passed version.
func selectDatabaseConflictsQuery(version int) string {
	switch {
	case version < PostgresV16:
		return postgresDatabaseConflictsQuery15
	default:
		return postgresDatabaseConflictsQueryLatest
	}
}
