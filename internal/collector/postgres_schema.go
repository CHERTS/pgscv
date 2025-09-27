// Package collector is a pgSCV collectors
package collector

import (
	"fmt"
	"github.com/cherts/pgscv/internal/log"
	"github.com/cherts/pgscv/internal/model"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/net/context"
	"strconv"
	"strings"
	"sync"
)

// postgresSchemaCollector defines metric descriptors and stats store.
type postgresSchemaCollector struct {
	syscatalog   typedDesc
	nonpktables  typedDesc
	invalididx   typedDesc
	nonidxfkey   typedDesc
	redundantidx typedDesc
	sequences    typedDesc
	difftypefkey typedDesc
}

// NewPostgresSchemasCollector returns a new Collector exposing postgres schema stats. Stats are based on different
// sources inside system catalog.
func NewPostgresSchemasCollector(constLabels labels, settings model.CollectorSettings) (Collector, error) {
	return &postgresSchemaCollector{
		syscatalog: newBuiltinTypedDesc(
			descOpts{"postgres", "schema", "system_catalog_bytes", "Number of bytes occupied by system catalog.", 0},
			prometheus.GaugeValue,
			[]string{"database"}, constLabels,
			settings.Filters,
		),
		nonpktables: newBuiltinTypedDesc(
			descOpts{"postgres", "schema", "non_pk_tables", "Labeled information about tables with no primary or unique key constraints.", 0},
			prometheus.GaugeValue,
			[]string{"database", "schema", "table"}, constLabels,
			settings.Filters,
		),
		invalididx: newBuiltinTypedDesc(
			descOpts{"postgres", "schema", "invalid_indexes_bytes", "Number of bytes occupied by invalid index.", 0},
			prometheus.GaugeValue,
			[]string{"database", "schema", "table", "index"}, constLabels,
			settings.Filters,
		),
		nonidxfkey: newBuiltinTypedDesc(
			descOpts{"postgres", "schema", "non_indexed_fkeys", "Number of non-indexed FOREIGN key constraints.", 0},
			prometheus.GaugeValue,
			[]string{"database", "schema", "table", "columns", "constraint", "referenced"}, constLabels,
			settings.Filters,
		),
		redundantidx: newBuiltinTypedDesc(
			descOpts{"postgres", "schema", "redundant_indexes_bytes", "Number of bytes occupied by redundant indexes.", 0},
			prometheus.GaugeValue,
			[]string{"database", "schema", "table", "index", "indexdef", "redundantdef"}, constLabels,
			settings.Filters,
		),
		sequences: newBuiltinTypedDesc(
			descOpts{"postgres", "schema", "sequence_exhaustion_ratio", "Sequences usage percentage accordingly to attached column, in percent.", 0},
			prometheus.GaugeValue,
			[]string{"database", "schema", "sequence"}, constLabels,
			settings.Filters,
		),
		difftypefkey: newBuiltinTypedDesc(
			descOpts{"postgres", "schema", "mistyped_fkeys", "Number of foreign key constraints with different data type.", 0},
			prometheus.GaugeValue,
			[]string{"database", "schema", "table", "column", "refschema", "reftable", "refcolumn"}, constLabels,
			settings.Filters,
		),
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *postgresSchemaCollector) Update(ctx context.Context, config Config, ch chan<- prometheus.Metric) error {
	wg := &sync.WaitGroup{}
	defer wg.Wait()

	// 1. get system catalog size in bytes.
	collectSystemCatalogSize(ctx, config, ch, wg, c.syscatalog)

	// 2. collect metrics related to tables with no primary/unique key constraints.
	collectSchemaNonPKTables(ctx, config, ch, wg, c.nonpktables)

	// Functions below uses queries with casting to regnamespace data type, which is introduced in Postgres 9.5.
	if config.pgVersion.Numeric < PostgresV95 {
		log.Debugln("[postgres schema collector]: some system data types are not available, required Postgres 9.5 or newer")
		return nil
	}

	// 3. collect metrics related to invalid indexes.
	collectSchemaInvalidIndexes(ctx, config, ch, wg, c.invalididx)

	// 4. collect metrics related to non indexed foreign key constraints.
	collectSchemaNonIndexedFK(ctx, config, ch, wg, c.nonidxfkey)

	// 5. collect metric related to redundant indexes.
	collectSchemaRedundantIndexes(ctx, config, ch, wg, c.redundantidx)

	// 6. collect metrics related to foreign key constraints with different data types.
	collectSchemaFKDatatypeMismatch(ctx, config, ch, wg, c.difftypefkey)

	// Function below uses queries pg_sequences which is introduced in Postgres 10.
	if config.pgVersion.Numeric < PostgresV10 {
		log.Debugln("[postgres schema collector]: some system views are not available, required Postgres 10 or newer")
	} else {
		// 7. collect metrics related to sequences (available since Postgres 10).
		collectSchemaSequences(ctx, config, ch, wg, c.sequences)
	}
	return nil
}

// collectSystemCatalogSize collects system catalog size metrics.
func collectSystemCatalogSize(ctx context.Context, config Config, ch chan<- prometheus.Metric, wg *sync.WaitGroup, desc typedDesc) {
	conn := config.DB
	datname := conn.Conn().Config().ConnConfig.Database
	size, err := getSystemCatalogSize(ctx, config, wg)
	if err != nil {
		log.Errorf("get system catalog size of database %s failed: %s; skip", datname, err)
		return
	}

	if size > 0 {
		ch <- desc.newConstMetric(size, datname)
	}
}

// getSystemCatalogSize returns size of system catalog in bytes.
func getSystemCatalogSize(ctx context.Context, config Config, wg *sync.WaitGroup) (float64, error) {
	conn := config.DB
	var query = `SELECT sum(pg_total_relation_size(relname::regclass)) AS bytes FROM pg_stat_sys_tables WHERE schemaname = 'pg_catalog'`
	var err error
	cacheKey, res, _ := getFromCache(config.CacheConfig, config.ConnString, collectorPostgresSchemas, query)
	if res == nil {
		res, err = conn.Query(ctx, query)
		if err != nil {
			return 0, err
		}
		saveToCache(collectorPostgresSchemas, wg, config.CacheConfig, cacheKey, res)
	}
	rows := res.Rows
	if len(rows) == 0 {
		return 0, fmt.Errorf("error get system catalog size")
	}
	row := rows[0]
	size, err := strconv.ParseFloat(row[0].String, 64)
	if err != nil {
		return 0, err
	}
	return size, nil
}

// collectSchemaNonPKTables collects metrics related to non-PK tables.
func collectSchemaNonPKTables(ctx context.Context, config Config, ch chan<- prometheus.Metric, wg *sync.WaitGroup, desc typedDesc) {
	conn := config.DB
	datname := conn.Conn().Config().ConnConfig.Database
	tables, err := getSchemaNonPKTables(ctx, config, wg)
	if err != nil {
		log.Errorf("collect non-pk tables in database %s failed: %s; skip", datname, err)
		return
	}

	for _, t := range tables {
		// tables are the slice of strings where each string is the table's FQN in following format: schemaname/relname
		parts := strings.Split(t, "/")
		if len(parts) != 2 {
			log.Warnf("incorrect table FQ name: %s; skip", t)
			continue
		}
		ch <- desc.newConstMetric(1, datname, parts[0], parts[1])
	}
}

// getSchemaNonPKTables searches tables with no PRIMARY or UNIQUE keys in the database and return its names.
func getSchemaNonPKTables(ctx context.Context, config Config, wg *sync.WaitGroup) ([]string, error) {
	conn := config.DB
	var query = "SELECT n.nspname AS schema, c.relname AS table " +
		"FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid " +
		"WHERE NOT EXISTS (SELECT 1 FROM pg_index i WHERE c.oid = i.indrelid AND (i.indisprimary OR i.indisunique)) " +
		"AND c.relkind = 'r' AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')"
	var err error
	cacheKey, res, _ := getFromCache(config.CacheConfig, config.ConnString, collectorPostgresSchemas, query)
	if res == nil {
		res, err = conn.Query(ctx, query)
		if err != nil {
			return nil, err
		}
		saveToCache(collectorPostgresSchemas, wg, config.CacheConfig, cacheKey, res)
	}
	rows := res.Rows

	var tables = []string{}
	var schemaname, relname, tableFQName string

	for _, row := range rows {
		schemaname = row[0].String
		relname = row[1].String
		tableFQName = schemaname + "/" + relname
		tables = append(tables, tableFQName)
	}

	return tables, nil
}

// collectSchemaInvalidIndexes collects metrics related to invalid indexes.
func collectSchemaInvalidIndexes(ctx context.Context, config Config, ch chan<- prometheus.Metric, wg *sync.WaitGroup, desc typedDesc) {
	database := config.DB.Conn().Config().ConnConfig.Database
	stats, err := getSchemaInvalidIndexes(ctx, config, wg)
	if err != nil {
		log.Errorf("get invalid indexes stats of database %s failed: %s; skip", database, err)
		return
	}

	for k, s := range stats {
		var (
			schema = s.labels["schema"]
			table  = s.labels["table"]
			index  = s.labels["index"]
			value  = s.values["bytes"]
		)

		if schema == "" || table == "" || index == "" {
			log.Warnf("incomplete invalid index FQ name: %s; skip", k)
			continue
		}

		ch <- desc.newConstMetric(value, database, schema, table, index)
	}
}

// getSchemaInvalidIndexes searches invalid indexes in the database and return its names if such indexes have been found.
func getSchemaInvalidIndexes(ctx context.Context, config Config, wg *sync.WaitGroup) (map[string]postgresGenericStat, error) {
	conn := config.DB
	var query = "SELECT c1.relnamespace::regnamespace::text AS schema, c2.relname AS table, c1.relname AS index, " +
		"pg_relation_size(i.indexrelid) AS bytes " +
		"FROM pg_index i JOIN pg_class c1 ON i.indexrelid = c1.oid JOIN pg_class c2 ON i.indrelid = c2.oid WHERE NOT i.indisvalid"
	var err error
	cacheKey, res, _ := getFromCache(config.CacheConfig, config.ConnString, collectorPostgresSchemas, query)
	if res == nil {
		res, err = conn.Query(ctx, query)
		if err != nil {
			return nil, err
		}
		saveToCache(collectorPostgresSchemas, wg, config.CacheConfig, cacheKey, res)
	}
	return parsePostgresGenericStats(res, []string{"schema", "table", "index"}), nil
}

// collectSchemaNonIndexedFK collects metrics related to non indexed foreign key constraints.
func collectSchemaNonIndexedFK(ctx context.Context, config Config, ch chan<- prometheus.Metric, wg *sync.WaitGroup, desc typedDesc) {
	database := config.DB.Conn().Config().ConnConfig.Database
	stats, err := getSchemaNonIndexedFK(ctx, config, wg)
	if err != nil {
		log.Errorf("get non-indexed fkeys stats of database %s failed: %s; skip", database, err)
		return
	}

	for k, s := range stats {
		var (
			schema     = s.labels["schema"]
			table      = s.labels["table"]
			columns    = s.labels["columns"]
			constraint = s.labels["constraint"]
			referenced = s.labels["referenced"]
		)

		if schema == "" || table == "" || columns == "" || constraint == "" || referenced == "" {
			log.Warnf("incomplete non-indexed foreign key constraint name: %s; skip", k)
			continue
		}

		ch <- desc.newConstMetric(1, database, schema, table, columns, constraint, referenced)
	}
}

// getSchemaNonIndexedFK searches non indexes foreign key constraints and return its names.
func getSchemaNonIndexedFK(ctx context.Context, config Config, wg *sync.WaitGroup) (map[string]postgresGenericStat, error) {
	conn := config.DB
	var err error
	var query = "SELECT c.connamespace::regnamespace::text AS schema, s.relname AS table, " +
		"string_agg(a.attname, ',' ORDER BY x.n) AS columns, c.conname AS constraint, " +
		"c.confrelid::regclass::text AS referenced " +
		"FROM pg_constraint c CROSS JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS x(attnum, n) " +
		"JOIN pg_attribute a ON a.attnum = x.attnum AND a.attrelid = c.conrelid " +
		"JOIN pg_class s ON c.conrelid = s.oid " +
		"WHERE NOT EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = c.conrelid AND (i.indkey::integer[])[0:cardinality(c.conkey)-1] @> c.conkey::integer[]) " +
		"AND c.contype = 'f' " +
		"GROUP BY c.connamespace,s.relname,c.conname,c.confrelid"
	cacheKey, res, _ := getFromCache(config.CacheConfig, config.ConnString, collectorPostgresSchemas, query)
	if res == nil {
		res, err = conn.Query(ctx, query)
		if err != nil {
			return nil, err
		}
		saveToCache(collectorPostgresSchemas, wg, config.CacheConfig, cacheKey, res)
	}
	return parsePostgresGenericStats(res, []string{"schema", "table", "columns", "constraint", "referenced"}), nil
}

// collectSchemaRedundantIndexes collects metrics related to invalid indexes
func collectSchemaRedundantIndexes(ctx context.Context, config Config, ch chan<- prometheus.Metric, wg *sync.WaitGroup, desc typedDesc) {
	database := config.DB.Conn().Config().ConnConfig.Database
	stats, err := getSchemaRedundantIndexes(ctx, config, wg)
	if err != nil {
		log.Errorf("get redundant indexes stats of database %s failed: %s; skip", database, err)
		return
	}

	for k, s := range stats {
		var (
			schema       = s.labels["schema"]
			table        = s.labels["table"]
			index        = s.labels["index"]
			indexdef     = s.labels["indexdef"]
			redundantdef = s.labels["redundantdef"]
			value        = s.values["bytes"]
		)

		if schema == "" || table == "" || index == "" || indexdef == "" || redundantdef == "" {
			log.Warnf("incomplete redundant index name: %s; skip", k)
			continue
		}

		ch <- desc.newConstMetric(value, database, schema, table, index, indexdef, redundantdef)
	}
}

// getSchemaRedundantIndexes searches redundant indexes and returns its sizes
func getSchemaRedundantIndexes(ctx context.Context, config Config, wg *sync.WaitGroup) (map[string]postgresGenericStat, error) {
	var query = "WITH index_data AS (SELECT *, string_to_array(indkey::text,' ') AS key_array, array_length(string_to_array(indkey::text,' '),1) AS nkeys FROM pg_index) " +
		"SELECT c1.relnamespace::regnamespace::text AS schema, c1.relname AS table, c2.relname AS index, " +
		"pg_get_indexdef(i1.indexrelid) AS indexdef, pg_get_indexdef(i2.indexrelid) AS redundantdef, " +
		"pg_relation_size(i2.indexrelid) AS bytes " +
		"FROM index_data AS i1 JOIN index_data AS i2 ON i1.indrelid = i2.indrelid AND i1.indexrelid<>i2.indexrelid " +
		"JOIN pg_class c1 ON i1.indrelid = c1.oid " +
		"JOIN pg_class c2 ON i2.indexrelid = c2.oid " +
		`WHERE (regexp_replace(i1.indpred, 'location \\d+', 'location', 'g') IS NOT DISTINCT FROM regexp_replace(i2.indpred, 'location \\d+', 'location', 'g')) ` +
		`AND (regexp_replace(i1.indexprs, 'location \\d+', 'location', 'g') IS NOT DISTINCT FROM regexp_replace(i2.indexprs, 'location \\d+', 'location', 'g')) ` +
		"AND ((i1.nkeys > i2.nkeys AND NOT i2.indisunique) OR (i1.nkeys = i2.nkeys AND ((i1.indisunique AND i2.indisunique AND (i1.indexrelid>i2.indexrelid)) " +
		"OR (NOT i1.indisunique AND NOT i2.indisunique AND (i1.indexrelid>i2.indexrelid)) " +
		"OR (i1.indisunique AND NOT i2.indisunique)))) AND i1.key_array[1:i2.nkeys]=i2.key_array"
	var err error
	conn := config.DB
	cacheKey, res, _ := getFromCache(config.CacheConfig, config.ConnString, collectorPostgresSchemas, query)
	if res == nil {
		res, err = conn.Query(ctx, query)
		if err != nil {
			return nil, err
		}
		saveToCache(collectorPostgresSchemas, wg, config.CacheConfig, cacheKey, res)
	}

	return parsePostgresGenericStats(res, []string{"schema", "table", "index", "indexdef", "redundantdef"}), nil
}

// collectSchemaSequences collects metrics related to sequences attached to poor-typed columns.
func collectSchemaSequences(ctx context.Context, config Config, ch chan<- prometheus.Metric, wg *sync.WaitGroup, desc typedDesc) {
	database := config.DB.Conn().Config().ConnConfig.Database
	stats, err := getSchemaSequences(ctx, config, wg)
	if err != nil {
		log.Errorf("get sequences stats of database %s failed: %s; skip", database, err)
		return
	}

	for k, s := range stats {
		var (
			schema   = s.labels["schema"]
			sequence = s.labels["sequence"]
			value    = s.values["ratio"]
		)

		if schema == "" || sequence == "" {
			log.Warnf("incomplete sequence FQ name: %s; skip", k)
			continue
		}

		ch <- desc.newConstMetric(value, database, schema, sequence)
	}
}

// getSchemaSequences searches sequences attached to the poor-typed columns with risk of exhaustion.
func getSchemaSequences(ctx context.Context, config Config, wg *sync.WaitGroup) (map[string]postgresGenericStat, error) {
	var query = `SELECT schemaname AS schema, sequencename AS sequence, COALESCE(last_value, 0) / max_value::float AS ratio FROM pg_sequences`
	var err error
	conn := config.DB
	cacheKey, res, _ := getFromCache(config.CacheConfig, config.ConnString, collectorPostgresSchemas, query)
	if res == nil {
		res, err = conn.Query(ctx, query)
		if err != nil {
			return nil, err
		}
		saveToCache(collectorPostgresSchemas, wg, config.CacheConfig, cacheKey, res)
	}
	return parsePostgresGenericStats(res, []string{"schema", "sequence"}), nil
}

// collectSchemaFKDatatypeMismatch collects metrics related to foreign key constraints with different data types.
func collectSchemaFKDatatypeMismatch(ctx context.Context, config Config, ch chan<- prometheus.Metric, wg *sync.WaitGroup, desc typedDesc) {
	database := config.DB.Conn().Config().ConnConfig.Database
	stats, err := getSchemaFKDatatypeMismatch(ctx, config, wg)
	if err != nil {
		log.Errorf("get foreign keys data types stats of database %s failed: %s; skip", database, err)
		return
	}

	for k, s := range stats {
		var (
			schema    = s.labels["schema"]
			table     = s.labels["table"]
			column    = s.labels["column"]
			refschema = s.labels["refschema"]
			reftable  = s.labels["reftable"]
			refcolumn = s.labels["refcolumn"]
		)

		if schema == "" || table == "" || column == "" || refschema == "" || reftable == "" || refcolumn == "" {
			log.Warnf("incomplete FQ name %s in database %s; skip", k, database)
			continue
		}

		ch <- desc.newConstMetric(1, database, schema, table, column, refschema, reftable, refcolumn)
	}
}

// getSchemaFKDatatypeMismatch searches foreign key constraints with different data types.
func getSchemaFKDatatypeMismatch(ctx context.Context, config Config, wg *sync.WaitGroup) (map[string]postgresGenericStat, error) {
	var query = "SELECT c1.relnamespace::regnamespace::text AS schema, c1.relname AS table, a1.attname||'::'||t1.typname AS column, " +
		"c2.relnamespace::regnamespace::text AS refschema, c2.relname AS reftable, a2.attname||'::'||t2.typname AS refcolumn " +
		"FROM pg_constraint JOIN pg_class c1 ON c1.oid = conrelid JOIN pg_class c2 ON c2.oid = confrelid " +
		"JOIN pg_attribute a1 ON a1.attnum = conkey[1] AND a1.attrelid = conrelid " +
		"JOIN pg_attribute a2 ON a2.attnum = confkey[1] AND a2.attrelid = confrelid " +
		"JOIN pg_type t1 ON t1.oid = a1.atttypid " +
		"JOIN pg_type t2 ON t2.oid = a2.atttypid " +
		"WHERE a1.atttypid <> a2.atttypid AND contype = 'f'"
	var err error
	conn := config.DB
	cacheKey, res, _ := getFromCache(config.CacheConfig, config.ConnString, collectorPostgresSchemas, query)
	if res == nil {
		res, err = conn.Query(ctx, query)
		if err != nil {
			return nil, err
		}
		saveToCache(collectorPostgresSchemas, wg, config.CacheConfig, cacheKey, res)
	}
	return parsePostgresGenericStats(res, []string{"schema", "table", "column", "refschema", "reftable", "refcolumn"}), nil
}
