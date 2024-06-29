package collector

import (
	"strconv"
	"strings"

	"github.com/cherts/pgscv/internal/log"
	"github.com/cherts/pgscv/internal/model"
	"github.com/cherts/pgscv/internal/store"
	"github.com/jackc/pgx/v4"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	bloatTablesQuery = "WITH constants AS (SELECT current_setting('block_size')::numeric bs, 23 hdr, 8 ma), no_stats AS (" +
		"SELECT	table_schema, table_name, n_live_tup::numeric est_rows,	pg_table_size(relid)::numeric table_size " +
		"FROM information_schema.columns JOIN pg_stat_user_tables psut ON table_schema = psut.schemaname AND table_name = psut.relname " +
		"LEFT JOIN pg_stats	ON table_schema = pg_stats.schemaname AND table_name = pg_stats.tablename AND column_name = attname " +
		"WHERE attname IS NULL AND table_schema NOT IN ('pg_catalog', 'information_schema') GROUP BY table_schema, table_name, relid, n_live_tup)," +
		"null_headers AS (SELECT hdr + 1 + sum(CASE WHEN null_frac <> 0 THEN 1 ELSE 0 END) / 8 nullhdr,	sum((1 - null_frac) * avg_width) datawidth, " +
		"max(null_frac) maxfracsum,	schemaname,	tablename, hdr, ma,	bs FROM pg_stats CROSS JOIN constants LEFT JOIN no_stats ON schemaname = no_stats.table_schema AND " +
		"tablename = no_stats.table_name WHERE schemaname NOT IN ('pg_catalog', 'information_schema') AND no_stats.table_name IS NULL AND EXISTS(" +
		"SELECT	1 FROM information_schema.columns WHERE	schemaname = columns.table_schema AND tablename = columns.table_name) GROUP BY " +
		"schemaname, tablename,	hdr, ma, bs), data_headers AS (SELECT ma, bs, hdr, schemaname, tablename,(datawidth + (hdr + ma - CASE WHEN hdr % ma = 0 THEN ma ELSE hdr % ma END))::numeric datahdr, " +
		"maxfracsum * (nullhdr + ma - CASE WHEN nullhdr % ma = 0 THEN ma ELSE nullhdr % ma END) nullhdr2 FROM null_headers), table_estimates AS (" +
		"SELECT	schemaname,	tablename, bs, reltuples::numeric est_rows,	relpages * bs table_bytes, ceil(reltuples * (datahdr + nullhdr2 + 4 + ma - CASE WHEN datahdr % ma = 0 THEN ma ELSE datahdr % ma END) / (bs - 20)) * bs expected_bytes, " +
		"reltoastrelid FROM	data_headers JOIN pg_class ON tablename = relname JOIN pg_namespace	ON relnamespace = pg_namespace.oid AND schemaname = nspname " +
		"WHERE pg_class.relkind = 'r'), estimates_with_toast AS (SELECT	schemaname,	tablename, est_rows, table_bytes + coalesce(toast.relpages, 0) * bs table_bytes, " +
		"expected_bytes + ceil(coalesce(toast.reltuples, 0) / 4) * bs expected_bytes FROM table_estimates LEFT JOIN pg_class toast ON table_estimates.reltoastrelid = toast.oid AND	toast.relkind = 't'), " +
		"table_estimates_plus AS (SELECT current_database() databasename, schemaname, tablename, est_rows, CASE WHEN table_bytes > 0 THEN table_bytes::numeric ELSE NULL::numeric END table_bytes, " +
		"CASE WHEN expected_bytes > 0 THEN expected_bytes::numeric ELSE NULL::numeric END expected_bytes, " +
		"CASE WHEN expected_bytes > 0 AND table_bytes > 0 AND expected_bytes <= table_bytes THEN (table_bytes - expected_bytes)::numeric ELSE 0::numeric END bloat_bytes " +
		"FROM estimates_with_toast UNION ALL SELECT	current_database() databasename, table_schema, table_name, est_rows, table_size, NULL::numeric, NULL::numeric FROM no_stats), " +
		"bloat_data AS (SELECT current_database() databasename, schemaname, tablename, table_bytes, expected_bytes, round(bloat_bytes * 100 / table_bytes) pct_bloat, bloat_bytes, est_rows " +
		"FROM table_estimates_plus)	SELECT databasename AS database, schemaname AS schema, tablename AS table, est_rows, pct_bloat, bloat_bytes, table_bytes FROM bloat_data ORDER BY pct_bloat DESC;"
)

// postgresBloatTablesCollector defines metric descriptors and stats store.
type postgresBloatTablesCollector struct {
	estRows    typedDesc
	pctBloat   typedDesc
	bloatBytes typedDesc
	tableBytes typedDesc
	labelNames []string
}

// NewPostgresBloatTablesCollector returns a new Collector exposing postgres tables stats.
func NewPostgresBloatTablesCollector(constLabels labels, settings model.CollectorSettings) (Collector, error) {
	var labels = []string{"database", "schema", "table"}

	return &postgresBloatTablesCollector{
		labelNames: labels,
		estRows: newBuiltinTypedDesc(
			descOpts{"postgres", "table_bloat", "est_rows", "Number of rows in the table based on pg_class.reltuples value.", 0},
			prometheus.GaugeValue,
			labels, constLabels,
			settings.Filters,
		),
		pctBloat: newBuiltinTypedDesc(
			descOpts{"postgres", "table_bloat", "pct_bloat", "Total table bloat in percent.", 0},
			prometheus.GaugeValue,
			labels, constLabels,
			settings.Filters,
		),
		bloatBytes: newBuiltinTypedDesc(
			descOpts{"postgres", "table_bloat", "bloat_bytes", "Total table bloat in bytes.", 0},
			prometheus.GaugeValue,
			labels, constLabels,
			settings.Filters,
		),
		tableBytes: newBuiltinTypedDesc(
			descOpts{"postgres", "table_bloat", "table_bytes", "Total size of the table (including all forks and TOASTed data), in bytes.", 0},
			prometheus.GaugeValue,
			labels, constLabels,
			settings.Filters,
		),
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *postgresBloatTablesCollector) Update(config Config, ch chan<- prometheus.Metric) error {
	conn, err := store.New(config.ConnString)
	if err != nil {
		return err
	}

	databases, err := listDatabases(conn)
	if err != nil {
		return err
	}

	conn.Close()

	pgconfig, err := pgx.ParseConfig(config.ConnString)
	if err != nil {
		return err
	}

	for _, d := range databases {
		// Skip database if not matched to allowed.
		if config.DatabasesRE != nil && !config.DatabasesRE.MatchString(d) {
			continue
		}

		pgconfig.Database = d
		conn, err := store.NewWithConfig(pgconfig)
		if err != nil {
			return err
		}

		res, err := conn.Query(bloatTablesQuery)
		conn.Close()
		if err != nil {
			log.Warnf("get bloat tables stat of database '%s' failed: %s; skip", d, err)
			continue
		}

		stats := parsePostgresBloatTableStats(res, c.labelNames)

		for _, stat := range stats {
			// scan stats
			ch <- c.estRows.newConstMetric(stat.estrows, stat.database, stat.schema, stat.table)
			ch <- c.pctBloat.newConstMetric(stat.pctbloat, stat.database, stat.schema, stat.table)
			ch <- c.bloatBytes.newConstMetric(stat.bloatbytes, stat.database, stat.schema, stat.table)
			ch <- c.tableBytes.newConstMetric(stat.tablebytes, stat.database, stat.schema, stat.table)
		}
	}

	return nil
}

// postgresBloatTableStat is per-table store for metrics related to how tables are accessed.
type postgresBloatTableStat struct {
	database   string
	schema     string
	table      string
	estrows    float64
	pctbloat   float64
	bloatbytes float64
	tablebytes float64
}

// parsePostgresBloatTableStats parses PGResult and returns structs with stats values.
func parsePostgresBloatTableStats(r *model.PGResult, labelNames []string) map[string]postgresBloatTableStat {
	log.Debug("parse postgres bloat tables stats")

	var stats = make(map[string]postgresBloatTableStat)

	var tablename string

	for _, row := range r.Rows {
		table := postgresBloatTableStat{}
		for i, colname := range r.Colnames {
			switch string(colname.Name) {
			case "database":
				table.database = row[i].String
			case "schema":
				table.schema = row[i].String
			case "table":
				table.table = row[i].String
			}
		}

		// create a table name consisting of trio database/schema/table
		tablename = strings.Join([]string{table.database, table.schema, table.table}, "/")

		stats[tablename] = table

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

			s := stats[tablename]

			switch string(colname.Name) {
			case "est_rows":
				s.estrows = v
			case "pct_bloat":
				s.pctbloat = v
			case "bloat_bytes":
				s.bloatbytes = v
			case "table_bytes":
				s.tablebytes = v
			default:
				continue
			}

			stats[tablename] = s
		}
	}

	return stats
}
