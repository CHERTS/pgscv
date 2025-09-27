// Package collector is a pgSCV collectors
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
	userIndexesQuery = "SELECT current_database() AS database, schemaname AS schema, relname AS table, indexrelname AS index, (i.indisprimary OR i.indisunique) AS key," +
		"i.indisvalid AS isvalid, idx_scan, idx_tup_read, idx_tup_fetch, idx_blks_read, idx_blks_hit, pg_relation_size(s1.indexrelid) AS size_bytes " +
		"FROM pg_stat_user_indexes s1 " +
		"JOIN pg_statio_user_indexes s2 USING (schemaname, relname, indexrelname) " +
		"JOIN pg_index i ON (s1.indexrelid = i.indexrelid) " +
		"WHERE NOT EXISTS (SELECT 1 FROM pg_locks WHERE relation = s1.indexrelid AND mode = 'AccessExclusiveLock' AND granted)"

	userIndexesQueryTopK = "WITH stat AS (SELECT schemaname AS schema, relname AS table, indexrelname AS index, (i.indisprimary OR i.indisunique) AS key, " +
		"i.indisvalid AS isvalid, idx_scan, idx_tup_read, idx_tup_fetch, idx_blks_read, idx_blks_hit, pg_relation_size(s1.indexrelid) AS size_bytes, " +
		"NOT i.indisvalid OR /* unused and size > 50mb */ (idx_scan = 0 AND pg_relation_size(s1.indexrelid) > 50*1024*1024) OR " +
		"(row_number() OVER (ORDER BY idx_scan DESC NULLS LAST) < $1) OR (row_number() OVER (ORDER BY idx_tup_read DESC NULLS LAST) < $1) OR " +
		"(row_number() OVER (ORDER BY idx_tup_fetch DESC NULLS LAST) < $1) OR (row_number() OVER (ORDER BY idx_blks_read DESC NULLS LAST) < $1) OR " +
		"(row_number() OVER (ORDER BY idx_blks_hit DESC NULLS LAST) < $1) OR (row_number() OVER (ORDER BY pg_relation_size(s1.indexrelid) DESC NULLS LAST) < $1) AS visible " +
		"FROM pg_stat_user_indexes s1 " +
		"JOIN pg_statio_user_indexes s2 USING (schemaname, relname, indexrelname) " +
		"JOIN pg_index i ON (s1.indexrelid = i.indexrelid) " +
		"WHERE NOT EXISTS ( SELECT 1 FROM pg_locks WHERE relation = s1.indexrelid AND mode = 'AccessExclusiveLock' AND granted)) " +
		"SELECT current_database() AS database, \"schema\", \"table\", \"index\", \"key\", isvalid, idx_scan, idx_tup_read, idx_tup_fetch, " +
		"idx_blks_read, idx_blks_hit, size_bytes FROM stat WHERE visible " +
		"UNION ALL SELECT current_database() AS database, 'all_shemas', 'all_other_tables', 'all_other_indexes', true, null, " +
		"NULLIF(SUM(COALESCE(idx_scan,0)),0), NULLIF(SUM(COALESCE(idx_tup_fetch,0)),0), NULLIF(SUM(COALESCE(idx_tup_read,0)),0), " +
		"NULLIF(SUM(COALESCE(idx_blks_read,0)),0), NULLIF(SUM(COALESCE(idx_blks_hit,0)),0), " +
		"NULLIF(SUM(COALESCE(size_bytes,0)),0) FROM stat WHERE NOT visible HAVING EXISTS (SELECT 1 FROM stat WHERE NOT visible)"
)

// postgresIndexesCollector defines metric descriptors and stats store.
type postgresIndexesCollector struct {
	indexes typedDesc
	tuples  typedDesc
	io      typedDesc
	sizes   typedDesc
}

// NewPostgresIndexesCollector returns a new Collector exposing postgres indexes stats.
// For details see
// https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STAT-ALL-INDEXES-VIEW
// https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STATIO-ALL-INDEXES-VIEW
func NewPostgresIndexesCollector(constLabels labels, settings model.CollectorSettings) (Collector, error) {
	return &postgresIndexesCollector{
		indexes: newBuiltinTypedDesc(
			descOpts{"postgres", "index", "scans_total", "Total number of index scans initiated.", 0},
			prometheus.CounterValue,
			[]string{"database", "schema", "table", "index", "key", "isvalid"}, constLabels,
			settings.Filters,
		),
		tuples: newBuiltinTypedDesc(
			descOpts{"postgres", "index", "tuples_total", "Total number of index entries processed by scans.", 0},
			prometheus.CounterValue,
			[]string{"database", "schema", "table", "index", "tuples"}, constLabels,
			settings.Filters,
		),
		io: newBuiltinTypedDesc(
			descOpts{"postgres", "index_io", "blocks_total", "Total number of indexes blocks processed.", 0},
			prometheus.CounterValue,
			[]string{"database", "schema", "table", "index", "access"}, constLabels,
			settings.Filters,
		),
		sizes: newBuiltinTypedDesc(
			descOpts{"postgres", "index", "size_bytes", "Total size of the index, in bytes.", 0},
			prometheus.GaugeValue,
			[]string{"database", "schema", "table", "index"}, constLabels,
			settings.Filters,
		),
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *postgresIndexesCollector) Update(ctx context.Context, config Config, ch chan<- prometheus.Metric) error {
	var err error
	var cacheKey string
	conn := config.DB
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	var res *model.PGResult

	if config.CollectTopIndex > 0 {
		cacheKey, res, _ = getFromCache(config.CacheConfig, config.ConnString, collectorPostgresIndexes, userIndexesQueryTopK, config.CollectTopIndex)
		if res == nil {
			res, err = conn.Query(ctx, userIndexesQueryTopK, config.CollectTopIndex)
			if err != nil {
				return err
			}
			saveToCache(collectorPostgresIndexes, wg, config.CacheConfig, cacheKey, res)
		}
	} else {
		cacheKey, res, _ = getFromCache(config.CacheConfig, config.ConnString, collectorPostgresIndexes, userIndexesQuery)
		if res == nil {
			res, err = conn.Query(ctx, userIndexesQuery)
			if err != nil {
				return err
			}
			saveToCache(collectorPostgresIndexes, wg, config.CacheConfig, cacheKey, res)
		}
	}
	if err != nil {
		log.Warnf("get indexes stat failed: %s", err)
		return nil
	}

	stats := parsePostgresIndexStats(res, c.indexes.labelNames)

	for _, stat := range stats {
		// always send idx scan metrics and indexes size
		ch <- c.indexes.newConstMetric(stat.idxscan, stat.database, stat.schema, stat.table, stat.index, stat.key, stat.isvalid)
		ch <- c.sizes.newConstMetric(stat.sizebytes, stat.database, stat.schema, stat.table, stat.index)

		// avoid metrics spamming and send metrics only if they greater than zero.
		if stat.idxtupread > 0 {
			ch <- c.tuples.newConstMetric(stat.idxtupread, stat.database, stat.schema, stat.table, stat.index, "read")
		}
		if stat.idxtupfetch > 0 {
			ch <- c.tuples.newConstMetric(stat.idxtupfetch, stat.database, stat.schema, stat.table, stat.index, "fetched")
		}
		if stat.idxread > 0 {
			ch <- c.io.newConstMetric(stat.idxread, stat.database, stat.schema, stat.table, stat.index, "read")
		}
		if stat.idxhit > 0 {
			ch <- c.io.newConstMetric(stat.idxhit, stat.database, stat.schema, stat.table, stat.index, "hit")
		}
	}
	return nil
}

// postgresIndexStat is per-index store for metrics related to how indexes are accessed.
type postgresIndexStat struct {
	database    string
	schema      string
	table       string
	index       string
	key         string
	isvalid     string
	idxscan     float64
	idxtupread  float64
	idxtupfetch float64
	idxread     float64
	idxhit      float64
	sizebytes   float64
}

// parsePostgresIndexStats parses PGResult and returns structs with stats values.
func parsePostgresIndexStats(r *model.PGResult, labelNames []string) map[string]postgresIndexStat {
	log.Debug("parse postgres indexes stats")

	var stats = make(map[string]postgresIndexStat)

	var indexname string

	for _, row := range r.Rows {
		index := postgresIndexStat{}
		for i, colname := range r.Colnames {
			switch string(colname.Name) {
			case "database":
				index.database = row[i].String
			case "schema":
				index.schema = row[i].String
			case "table":
				index.table = row[i].String
			case "index":
				index.index = row[i].String
			case "key":
				index.key = row[i].String
			case "isvalid":
				index.isvalid = row[i].String
			}
		}

		// create a index name consisting of quartet database/schema/table/index
		indexname = strings.Join([]string{index.database, index.schema, index.table, index.index}, "/")

		stats[indexname] = index

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

			s := stats[indexname]

			switch string(colname.Name) {
			case "idx_scan":
				s.idxscan = v
			case "idx_tup_read":
				s.idxtupread = v
			case "idx_tup_fetch":
				s.idxtupfetch = v
			case "idx_blks_read":
				s.idxread = v
			case "idx_blks_hit":
				s.idxhit = v
			case "size_bytes":
				s.sizebytes = v
			default:
				continue
			}

			stats[indexname] = s
		}
	}

	return stats
}
