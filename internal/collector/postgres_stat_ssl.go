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
	postgresStatSslQueryLatest = "WITH pgsa AS ( " +
		"SELECT datname AS database, usename AS username, count(*) AS ssl_conn_number FROM pg_stat_ssl " +
		"JOIN pg_stat_activity ON pg_stat_activity.pid = pg_stat_ssl.pid " +
		"WHERE ssl = 't' AND datname IS NOT NULL GROUP BY datname, usename " +
		"), pgsr AS ( " +
		"SELECT COALESCE(NULL::text, 'NULL') AS database, usename AS username, count(*) AS ssl_conn_number FROM pg_stat_ssl " +
		"JOIN pg_stat_replication ON pg_stat_replication.pid = pg_stat_ssl.pid " +
		"WHERE ssl = 't' AND usename IS NOT NULL GROUP BY usename " +
		") SELECT * FROM pgsa " +
		"UNION " +
		"SELECT * FROM pgsr;"
)

// postgresStatSslCollector defines metric descriptors and stats store.
type postgresStatSslCollector struct {
	sslConnNumber typedDesc
	labelNames    []string
}

// NewPostgresStatSslCollector returns a new Collector exposing postgres pg_stat_ssl stats.
// For details see https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-SSL-VIEW
func NewPostgresStatSslCollector(constLabels labels, settings model.CollectorSettings) (Collector, error) {
	var labelNames = []string{"database", "username"}

	return &postgresStatSslCollector{
		labelNames: labelNames,
		sslConnNumber: newBuiltinTypedDesc(
			descOpts{"postgres", "stat_ssl", "conn_number", "Number of SSL connections.", 0},
			prometheus.GaugeValue,
			labelNames, constLabels,
			settings.Filters,
		),
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *postgresStatSslCollector) Update(ctx context.Context, config Config, ch chan<- prometheus.Metric) error {
	if config.pgVersion.Numeric < PostgresV95 {
		log.Debugln("[postgres stat_ssl collector]: pg_stat_ssl view are not available, required Postgres 9.5 or newer")
		return nil
	}

	conn := config.DB
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	var err error

	cacheKey, res, _ := getFromCache(config.CacheConfig, config.ConnString, collectorPostgresStatSSL, postgresStatSslQueryLatest)
	if res == nil {
		res, err = conn.Query(ctx, postgresStatSslQueryLatest)
		if err != nil {
			log.Warnf("get pg_stat_ssl failed: %s; skip", err)
			return err
		}
		saveToCache(collectorPostgresStatSSL, wg, config.CacheConfig, cacheKey, res)
	}

	// Parse pg_stat_ssl stats.
	stats := parsePostgresStatSsl(res, c.labelNames)
	for _, stat := range stats {
		ch <- c.sslConnNumber.newConstMetric(stat.ConnNumber, stat.Database, stat.Username)
	}

	return nil
}

// postgresStatSsl represents per-subscription stats based on pg_stat_ssl.
type postgresStatSsl struct {
	Database   string // a database
	Username   string // a username
	ConnNumber float64
}

// parsePostgresStatSsl parses PGResult and returns structs with stats values.
func parsePostgresStatSsl(r *model.PGResult, labelNames []string) map[string]postgresStatSsl {
	log.Debug("parse postgres pg_stat_ssl stats")

	var stats = make(map[string]postgresStatSsl)

	for _, row := range r.Rows {
		var Database, Username string

		for i, colname := range r.Colnames {
			switch string(colname.Name) {
			case "database":
				Database = row[i].String
			case "username":
				Username = row[i].String
			}
		}

		// create a stat_io name consisting of trio Database/Username
		statSsl := strings.Join([]string{Database, Username}, "/")

		// Put stats with labels (but with no data values yet) into stats store.
		if _, ok := stats[statSsl]; !ok {
			stats[statSsl] = postgresStatSsl{Database: Database, Username: Username}
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

			s := stats[statSsl]

			switch string(colname.Name) {
			case "ssl_conn_number":
				s.ConnNumber = v
			default:
				continue
			}

			stats[statSsl] = s
		}
	}

	return stats
}
