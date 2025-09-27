// Package collector is a pgSCV collectors
package collector

import (
	"context"
	"fmt"
	"github.com/cherts/pgscv/internal/log"
	"github.com/cherts/pgscv/internal/model"
	"github.com/prometheus/client_golang/prometheus"
	"strconv"
	"strings"
	"sync"
)

const (
	// postgresStatementsQuery12 defines query for querying statements metrics for PG12 and older.
	postgresStatementsQuery12 = "SELECT d.datname AS database, pg_get_userbyid(p.userid) AS \"user\", p.queryid, " +
		"COALESCE(%s, '') AS query, p.calls, p.rows, p.total_time, p.blk_read_time, p.blk_write_time, " +
		"NULLIF(p.shared_blks_hit, 0) AS shared_blks_hit, NULLIF(p.shared_blks_read, 0) AS shared_blks_read, " +
		"NULLIF(p.shared_blks_dirtied, 0) AS shared_blks_dirtied, NULLIF(p.shared_blks_written, 0) AS shared_blks_written, " +
		"NULLIF(p.local_blks_hit, 0) AS local_blks_hit, NULLIF(p.local_blks_read, 0) AS local_blks_read, " +
		"NULLIF(p.local_blks_dirtied, 0) AS local_blks_dirtied, NULLIF(p.local_blks_written, 0) AS local_blks_written, " +
		"NULLIF(p.temp_blks_read, 0) AS temp_blks_read, NULLIF(p.temp_blks_written, 0) AS temp_blks_written " +
		"FROM %s.pg_stat_statements p JOIN pg_database d ON d.oid=p.dbid;"

	postgresStatementsQuery12TopK = "WITH stat AS (SELECT d.datname AS DATABASE, pg_get_userbyid(p.userid) AS \"user\", p.queryid, " +
		"COALESCE(%s, '') AS query, p.calls, p.rows, p.total_time, p.blk_read_time, p.blk_write_time, " +
		"NULLIF(p.shared_blks_hit, 0) AS shared_blks_hit, NULLIF(p.shared_blks_read, 0) AS shared_blks_read, " +
		"NULLIF(p.shared_blks_dirtied, 0) AS shared_blks_dirtied, NULLIF(p.shared_blks_written, 0) AS shared_blks_written, " +
		"NULLIF(p.local_blks_hit, 0) AS local_blks_hit, NULLIF(p.local_blks_read, 0) AS local_blks_read, " +
		"NULLIF(p.local_blks_dirtied, 0) AS local_blks_dirtied, NULLIF(p.local_blks_written, 0) AS local_blks_written, " +
		"NULLIF(p.temp_blks_read, 0) AS temp_blks_read, NULLIF(p.temp_blks_written, 0) AS temp_blks_written, " +
		"(ROW_NUMBER() OVER ( ORDER BY p.calls DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.rows DESC NULLS LAST) < $1) OR " +
		"(ROW_NUMBER() OVER ( ORDER BY p.total_time DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.blk_read_time DESC NULLS LAST) < $1) OR " +
		"(ROW_NUMBER() OVER ( ORDER BY p.blk_read_time DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.blk_write_time DESC NULLS LAST) < $1) OR " +
		"(ROW_NUMBER() OVER ( ORDER BY p.shared_blks_hit DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.shared_blks_read DESC NULLS LAST) < $1) OR " +
		"(ROW_NUMBER() OVER ( ORDER BY p.shared_blks_dirtied DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.shared_blks_written DESC NULLS LAST) < $1) OR " +
		"(ROW_NUMBER() OVER ( ORDER BY p.local_blks_hit DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.local_blks_read DESC NULLS LAST) < $1) OR " +
		"(ROW_NUMBER() OVER ( ORDER BY p.local_blks_dirtied DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.local_blks_written DESC NULLS LAST) < $1) OR " +
		"(ROW_NUMBER() OVER ( ORDER BY p.temp_blks_read DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.temp_blks_written DESC NULLS LAST) < $1) AS visible " +
		"FROM %s.pg_stat_statements p JOIN pg_database d ON d.oid = p.dbid) " +
		"SELECT DATABASE, \"user\", queryid, query, calls, rows, total_time, blk_read_time, blk_write_time, shared_blks_hit, " +
		"shared_blks_read, shared_blks_dirtied, shared_blks_written, local_blks_hit, local_blks_read, local_blks_dirtied, local_blks_written, " +
		"temp_blks_read, temp_blks_written FROM stat WHERE visible UNION ALL SELECT DATABASE, 'all_users', NULL, " +
		"'all_queries', NULLIF(SUM(COALESCE(calls, 0)), 0), NULLIF(SUM(COALESCE(ROWS, 0)), 0), NULLIF(SUM(COALESCE(total_time, 0)), 0), " +
		"NULLIF(SUM(COALESCE(blk_read_time, 0)), 0), NULLIF(SUM(COALESCE(blk_write_time, 0)), 0), " +
		"NULLIF(SUM(COALESCE(shared_blks_hit, 0)), 0), NULLIF(SUM(COALESCE(shared_blks_read, 0)), 0), NULLIF(SUM(COALESCE(shared_blks_dirtied, 0)), 0), " +
		"NULLIF(SUM(COALESCE(shared_blks_written, 0)), 0), NULLIF(SUM(COALESCE(local_blks_hit, 0)), 0), NULLIF(SUM(COALESCE(local_blks_read, 0)), 0), " +
		"NULLIF(SUM(COALESCE(local_blks_dirtied, 0)), 0), NULLIF(SUM(COALESCE(local_blks_written, 0)), 0), NULLIF(SUM(COALESCE(temp_blks_read, 0)), 0), " +
		"NULLIF(SUM(COALESCE(temp_blks_written, 0)), 0) FROM stat WHERE NOT visible GROUP BY DATABASE HAVING EXISTS (SELECT 1 FROM stat WHERE NOT visible)"

	postgresStatementsQuery16 = "SELECT d.datname AS database, pg_get_userbyid(p.userid) AS \"user\", p.queryid, " +
		"COALESCE(%s, '') AS query, p.calls, p.rows, p.total_exec_time, p.total_plan_time, p.blk_read_time, p.blk_write_time, " +
		"NULLIF(p.shared_blks_hit, 0) AS shared_blks_hit, NULLIF(p.shared_blks_read, 0) AS shared_blks_read, " +
		"NULLIF(p.shared_blks_dirtied, 0) AS shared_blks_dirtied, NULLIF(p.shared_blks_written, 0) AS shared_blks_written, " +
		"NULLIF(p.local_blks_hit, 0) AS local_blks_hit, NULLIF(p.local_blks_read, 0) AS local_blks_read, " +
		"NULLIF(p.local_blks_dirtied, 0) AS local_blks_dirtied, NULLIF(p.local_blks_written, 0) AS local_blks_written, " +
		"NULLIF(p.temp_blks_read, 0) AS temp_blks_read, NULLIF(p.temp_blks_written, 0) AS temp_blks_written, " +
		"NULLIF(p.wal_records, 0) AS wal_records, NULLIF(p.wal_fpi, 0) AS wal_fpi, NULLIF(p.wal_bytes, 0) AS wal_bytes " +
		"FROM %s.pg_stat_statements p JOIN pg_database d ON d.oid=p.dbid"

	postgresStatementsQuery16TopK = "WITH stat AS (SELECT d.datname AS DATABASE, pg_get_userbyid(p.userid) AS \"user\", p.queryid, " +
		"COALESCE(%s, '') AS query, p.calls, p.rows, p.total_exec_time, p.total_plan_time, p.blk_read_time, p.blk_write_time, " +
		"NULLIF(p.shared_blks_hit, 0) AS shared_blks_hit, NULLIF(p.shared_blks_read, 0) AS shared_blks_read, " +
		"NULLIF(p.shared_blks_dirtied, 0) AS shared_blks_dirtied, NULLIF(p.shared_blks_written, 0) AS shared_blks_written, " +
		"NULLIF(p.local_blks_hit, 0) AS local_blks_hit, NULLIF(p.local_blks_read, 0) AS local_blks_read, " +
		"NULLIF(p.local_blks_dirtied, 0) AS local_blks_dirtied, NULLIF(p.local_blks_written, 0) AS local_blks_written, " +
		"NULLIF(p.temp_blks_read, 0) AS temp_blks_read, NULLIF(p.temp_blks_written, 0) AS temp_blks_written, " +
		"NULLIF(p.wal_records, 0) AS wal_records, NULLIF(p.wal_fpi, 0) AS wal_fpi, NULLIF(p.wal_bytes, 0) AS wal_bytes, " +
		"(ROW_NUMBER() OVER ( ORDER BY p.calls DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.rows DESC NULLS LAST) < $1) OR " +
		"(ROW_NUMBER() OVER ( ORDER BY p.total_exec_time DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.total_plan_time DESC NULLS LAST) < $1) OR " +
		"(ROW_NUMBER() OVER ( ORDER BY p.blk_read_time DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.blk_write_time DESC NULLS LAST) < $1) OR " +
		"(ROW_NUMBER() OVER ( ORDER BY p.shared_blks_hit DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.shared_blks_read DESC NULLS LAST) < $1) OR " +
		"(ROW_NUMBER() OVER ( ORDER BY p.shared_blks_dirtied DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.shared_blks_written DESC NULLS LAST) < $1) OR " +
		"(ROW_NUMBER() OVER ( ORDER BY p.local_blks_hit DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.local_blks_read DESC NULLS LAST) < $1) OR " +
		"(ROW_NUMBER() OVER ( ORDER BY p.local_blks_dirtied DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.local_blks_written DESC NULLS LAST) < $1) OR " +
		"(ROW_NUMBER() OVER ( ORDER BY p.temp_blks_read DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.temp_blks_written DESC NULLS LAST) < $1) OR " +
		"(ROW_NUMBER() OVER ( ORDER BY p.wal_records DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.wal_fpi DESC NULLS LAST) < $1) OR " +
		"(ROW_NUMBER() OVER ( ORDER BY p.wal_bytes DESC NULLS LAST) < $1) AS visible FROM %s.pg_stat_statements p JOIN pg_database d ON d.oid = p.dbid) " +
		"SELECT DATABASE, \"user\", queryid, query, calls, rows, total_exec_time, total_plan_time, blk_read_time, blk_write_time, shared_blks_hit, " +
		"shared_blks_read, shared_blks_dirtied, shared_blks_written, local_blks_hit, local_blks_read, local_blks_dirtied, local_blks_written, " +
		"temp_blks_read, temp_blks_written, wal_records, wal_fpi, wal_bytes FROM stat WHERE visible UNION ALL SELECT DATABASE, 'all_users', NULL, " +
		"'all_queries', NULLIF(SUM(COALESCE(calls, 0)), 0), NULLIF(SUM(COALESCE(ROWS, 0)), 0), NULLIF(SUM(COALESCE(total_exec_time, 0)), 0), " +
		"NULLIF(SUM(COALESCE(total_plan_time, 0)), 0), NULLIF(SUM(COALESCE(blk_read_time, 0)), 0), NULLIF(SUM(COALESCE(blk_write_time, 0)), 0), " +
		"NULLIF(SUM(COALESCE(shared_blks_hit, 0)), 0), NULLIF(SUM(COALESCE(shared_blks_read, 0)), 0), NULLIF(SUM(COALESCE(shared_blks_dirtied, 0)), 0), " +
		"NULLIF(SUM(COALESCE(shared_blks_written, 0)), 0), NULLIF(SUM(COALESCE(local_blks_hit, 0)), 0), NULLIF(SUM(COALESCE(local_blks_read, 0)), 0), " +
		"NULLIF(SUM(COALESCE(local_blks_dirtied, 0)), 0), NULLIF(SUM(COALESCE(local_blks_written, 0)), 0), NULLIF(SUM(COALESCE(temp_blks_read, 0)), 0), " +
		"NULLIF(SUM(COALESCE(temp_blks_written, 0)), 0), NULLIF(SUM(COALESCE(wal_records, 0)), 0), NULLIF(SUM(COALESCE(wal_fpi, 0)), 0), " +
		"NULLIF(SUM(COALESCE(wal_bytes, 0)), 0) FROM stat WHERE NOT visible GROUP BY DATABASE HAVING EXISTS (SELECT 1 FROM stat WHERE NOT visible)"

	postgresStatementsQuery17 = "SELECT d.datname AS database, pg_get_userbyid(p.userid) AS \"user\", p.queryid, " +
		"COALESCE(%s, '') AS query, p.calls, p.rows, p.total_exec_time, p.total_plan_time, p.shared_blk_read_time AS blk_read_time, " +
		"p.shared_blk_write_time AS blk_write_time, NULLIF(p.shared_blks_hit, 0) AS shared_blks_hit, NULLIF(p.shared_blks_read, 0) AS shared_blks_read, " +
		"NULLIF(p.shared_blks_dirtied, 0) AS shared_blks_dirtied, NULLIF(p.shared_blks_written, 0) AS shared_blks_written, " +
		"NULLIF(p.local_blks_hit, 0) AS local_blks_hit, NULLIF(p.local_blks_read, 0) AS local_blks_read, " +
		"NULLIF(p.local_blks_dirtied, 0) AS local_blks_dirtied, NULLIF(p.local_blks_written, 0) AS local_blks_written, " +
		"NULLIF(p.temp_blks_read, 0) AS temp_blks_read, NULLIF(p.temp_blks_written, 0) AS temp_blks_written, " +
		"NULLIF(p.wal_records, 0) AS wal_records, NULLIF(p.wal_fpi, 0) AS wal_fpi, NULLIF(p.wal_bytes, 0) AS wal_bytes " +
		"FROM %s.pg_stat_statements p JOIN pg_database d ON d.oid=p.dbid"

	postgresStatementsQuery17TopK = "WITH stat AS (SELECT d.datname AS DATABASE, pg_get_userbyid(p.userid) AS \"user\", p.queryid, " +
		"COALESCE(%s, '') AS query, p.calls, p.rows, p.total_exec_time, p.total_plan_time, p.shared_blk_read_time AS blk_read_time, " +
		"p.shared_blk_write_time AS blk_write_time, NULLIF(p.shared_blks_hit, 0) AS shared_blks_hit, NULLIF(p.shared_blks_read, 0) AS shared_blks_read, " +
		"NULLIF(p.shared_blks_dirtied, 0) AS shared_blks_dirtied, NULLIF(p.shared_blks_written, 0) AS shared_blks_written, " +
		"NULLIF(p.local_blks_hit, 0) AS local_blks_hit, NULLIF(p.local_blks_read, 0) AS local_blks_read, " +
		"NULLIF(p.local_blks_dirtied, 0) AS local_blks_dirtied, NULLIF(p.local_blks_written, 0) AS local_blks_written, " +
		"NULLIF(p.temp_blks_read, 0) AS temp_blks_read, NULLIF(p.temp_blks_written, 0) AS temp_blks_written, " +
		"NULLIF(p.wal_records, 0) AS wal_records, NULLIF(p.wal_fpi, 0) AS wal_fpi, NULLIF(p.wal_bytes, 0) AS wal_bytes, " +
		"(ROW_NUMBER() OVER ( ORDER BY p.calls DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.rows DESC NULLS LAST) < $1) OR " +
		"(ROW_NUMBER() OVER ( ORDER BY p.total_exec_time DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.total_plan_time DESC NULLS LAST) < $1) OR " +
		"(ROW_NUMBER() OVER ( ORDER BY p.shared_blk_read_time DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.shared_blk_write_time DESC NULLS LAST) < $1) OR " +
		"(ROW_NUMBER() OVER ( ORDER BY p.shared_blks_hit DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.shared_blks_read DESC NULLS LAST) < $1) OR " +
		"(ROW_NUMBER() OVER ( ORDER BY p.shared_blks_dirtied DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.shared_blks_written DESC NULLS LAST) < $1) OR " +
		"(ROW_NUMBER() OVER ( ORDER BY p.local_blks_hit DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.local_blks_read DESC NULLS LAST) < $1) OR " +
		"(ROW_NUMBER() OVER ( ORDER BY p.local_blks_dirtied DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.local_blks_written DESC NULLS LAST) < $1) OR " +
		"(ROW_NUMBER() OVER ( ORDER BY p.temp_blks_read DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.temp_blks_written DESC NULLS LAST) < $1) OR " +
		"(ROW_NUMBER() OVER ( ORDER BY p.wal_records DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.wal_fpi DESC NULLS LAST) < $1) OR " +
		"(ROW_NUMBER() OVER ( ORDER BY p.wal_bytes DESC NULLS LAST) < $1) AS visible FROM %s.pg_stat_statements p JOIN pg_database d ON d.oid = p.dbid) " +
		"SELECT DATABASE, \"user\", queryid, query, calls, rows, total_exec_time, total_plan_time, blk_read_time, blk_write_time, shared_blks_hit, " +
		"shared_blks_read, shared_blks_dirtied, shared_blks_written, local_blks_hit, local_blks_read, local_blks_dirtied, local_blks_written, " +
		"temp_blks_read, temp_blks_written, wal_records, wal_fpi, wal_bytes FROM stat WHERE visible UNION ALL SELECT DATABASE, 'all_users', NULL, " +
		"'all_queries', NULLIF(SUM(COALESCE(calls, 0)), 0), NULLIF(SUM(COALESCE(ROWS, 0)), 0), NULLIF(SUM(COALESCE(total_exec_time, 0)), 0), " +
		"NULLIF(SUM(COALESCE(total_plan_time, 0)), 0), NULLIF(SUM(COALESCE(blk_read_time, 0)), 0), NULLIF(SUM(COALESCE(blk_write_time, 0)), 0), " +
		"NULLIF(SUM(COALESCE(shared_blks_hit, 0)), 0), NULLIF(SUM(COALESCE(shared_blks_read, 0)), 0), NULLIF(SUM(COALESCE(shared_blks_dirtied, 0)), 0), " +
		"NULLIF(SUM(COALESCE(shared_blks_written, 0)), 0), NULLIF(SUM(COALESCE(local_blks_hit, 0)), 0), NULLIF(SUM(COALESCE(local_blks_read, 0)), 0), " +
		"NULLIF(SUM(COALESCE(local_blks_dirtied, 0)), 0), NULLIF(SUM(COALESCE(local_blks_written, 0)), 0), NULLIF(SUM(COALESCE(temp_blks_read, 0)), 0), " +
		"NULLIF(SUM(COALESCE(temp_blks_written, 0)), 0), NULLIF(SUM(COALESCE(wal_records, 0)), 0), NULLIF(SUM(COALESCE(wal_fpi, 0)), 0), " +
		"NULLIF(SUM(COALESCE(wal_bytes, 0)), 0) FROM stat WHERE NOT visible GROUP BY DATABASE HAVING EXISTS (SELECT 1 FROM stat WHERE NOT visible)"

	// postgresStatementsQueryLatest defines query for querying statements metrics.
	// 1. use nullif(value, 0) to nullify zero values, NULL are skipped by stats method and metrics wil not be generated.
	postgresStatementsQueryLatest = "SELECT d.datname AS database, pg_get_userbyid(p.userid) AS \"user\", p.queryid, " +
		"COALESCE(%s, '') AS query, p.calls, p.rows, p.total_exec_time, p.total_plan_time, p.shared_blk_read_time AS blk_read_time, " +
		"p.shared_blk_write_time AS blk_write_time, NULLIF(p.shared_blks_hit, 0) AS shared_blks_hit, NULLIF(p.shared_blks_read, 0) AS shared_blks_read, " +
		"NULLIF(p.shared_blks_dirtied, 0) AS shared_blks_dirtied, NULLIF(p.shared_blks_written, 0) AS shared_blks_written, " +
		"NULLIF(p.local_blks_hit, 0) AS local_blks_hit, NULLIF(p.local_blks_read, 0) AS local_blks_read, " +
		"NULLIF(p.local_blks_dirtied, 0) AS local_blks_dirtied, NULLIF(p.local_blks_written, 0) AS local_blks_written, " +
		"NULLIF(p.temp_blks_read, 0) AS temp_blks_read, NULLIF(p.temp_blks_written, 0) AS temp_blks_written, " +
		"NULLIF(p.wal_records, 0) AS wal_records, NULLIF(p.wal_fpi, 0) AS wal_fpi, NULLIF(p.wal_bytes, 0) AS wal_bytes, " +
		"NULLIF(p.wal_buffers_full, 0) AS wal_buffers_full " +
		"FROM %s.pg_stat_statements p JOIN pg_database d ON d.oid=p.dbid"

	postgresStatementsQueryLatestTopK = "WITH stat AS (SELECT d.datname AS DATABASE, pg_get_userbyid(p.userid) AS \"user\", p.queryid, " +
		"COALESCE(%s, '') AS query, p.calls, p.rows, p.total_exec_time, p.total_plan_time, p.shared_blk_read_time AS blk_read_time, " +
		"p.shared_blk_write_time AS blk_write_time, NULLIF(p.shared_blks_hit, 0) AS shared_blks_hit, NULLIF(p.shared_blks_read, 0) AS shared_blks_read, " +
		"NULLIF(p.shared_blks_dirtied, 0) AS shared_blks_dirtied, NULLIF(p.shared_blks_written, 0) AS shared_blks_written, " +
		"NULLIF(p.local_blks_hit, 0) AS local_blks_hit, NULLIF(p.local_blks_read, 0) AS local_blks_read, " +
		"NULLIF(p.local_blks_dirtied, 0) AS local_blks_dirtied, NULLIF(p.local_blks_written, 0) AS local_blks_written, " +
		"NULLIF(p.temp_blks_read, 0) AS temp_blks_read, NULLIF(p.temp_blks_written, 0) AS temp_blks_written, " +
		"NULLIF(p.wal_records, 0) AS wal_records, NULLIF(p.wal_fpi, 0) AS wal_fpi, NULLIF(p.wal_bytes, 0) AS wal_bytes, " +
		"NULLIF(p.wal_buffers_full, 0) AS wal_buffers_full, " +
		"(ROW_NUMBER() OVER ( ORDER BY p.calls DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.rows DESC NULLS LAST) < $1) OR " +
		"(ROW_NUMBER() OVER ( ORDER BY p.total_exec_time DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.total_plan_time DESC NULLS LAST) < $1) OR " +
		"(ROW_NUMBER() OVER ( ORDER BY p.shared_blk_read_time DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.shared_blk_write_time DESC NULLS LAST) < $1) OR " +
		"(ROW_NUMBER() OVER ( ORDER BY p.shared_blks_hit DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.shared_blks_read DESC NULLS LAST) < $1) OR " +
		"(ROW_NUMBER() OVER ( ORDER BY p.shared_blks_dirtied DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.shared_blks_written DESC NULLS LAST) < $1) OR " +
		"(ROW_NUMBER() OVER ( ORDER BY p.local_blks_hit DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.local_blks_read DESC NULLS LAST) < $1) OR " +
		"(ROW_NUMBER() OVER ( ORDER BY p.local_blks_dirtied DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.local_blks_written DESC NULLS LAST) < $1) OR " +
		"(ROW_NUMBER() OVER ( ORDER BY p.temp_blks_read DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.temp_blks_written DESC NULLS LAST) < $1) OR " +
		"(ROW_NUMBER() OVER ( ORDER BY p.wal_records DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.wal_fpi DESC NULLS LAST) < $1) OR " +
		"(ROW_NUMBER() OVER ( ORDER BY p.wal_bytes DESC NULLS LAST) < $1) OR (ROW_NUMBER() OVER ( ORDER BY p.wal_buffers_full DESC NULLS LAST) < $1) AS visible " +
		"FROM %s.pg_stat_statements p JOIN pg_database d ON d.oid = p.dbid) " +
		"SELECT DATABASE, \"user\", queryid, query, calls, rows, total_exec_time, total_plan_time, blk_read_time, blk_write_time, shared_blks_hit, " +
		"shared_blks_read, shared_blks_dirtied, shared_blks_written, local_blks_hit, local_blks_read, local_blks_dirtied, local_blks_written, " +
		"temp_blks_read, temp_blks_written, wal_records, wal_fpi, wal_bytes, wal_buffers_full FROM stat WHERE visible UNION ALL SELECT DATABASE, 'all_users', NULL, " +
		"'all_queries', NULLIF(SUM(COALESCE(calls, 0)), 0), NULLIF(SUM(COALESCE(ROWS, 0)), 0), NULLIF(SUM(COALESCE(total_exec_time, 0)), 0), " +
		"NULLIF(SUM(COALESCE(total_plan_time, 0)), 0), NULLIF(SUM(COALESCE(blk_read_time, 0)), 0), NULLIF(SUM(COALESCE(blk_write_time, 0)), 0), " +
		"NULLIF(SUM(COALESCE(shared_blks_hit, 0)), 0), NULLIF(SUM(COALESCE(shared_blks_read, 0)), 0), NULLIF(SUM(COALESCE(shared_blks_dirtied, 0)), 0), " +
		"NULLIF(SUM(COALESCE(shared_blks_written, 0)), 0), NULLIF(SUM(COALESCE(local_blks_hit, 0)), 0), NULLIF(SUM(COALESCE(local_blks_read, 0)), 0), " +
		"NULLIF(SUM(COALESCE(local_blks_dirtied, 0)), 0), NULLIF(SUM(COALESCE(local_blks_written, 0)), 0), NULLIF(SUM(COALESCE(temp_blks_read, 0)), 0), " +
		"NULLIF(SUM(COALESCE(temp_blks_written, 0)), 0), NULLIF(SUM(COALESCE(wal_records, 0)), 0), NULLIF(SUM(COALESCE(wal_fpi, 0)), 0), " +
		"NULLIF(SUM(COALESCE(wal_bytes, 0)), 0), NULLIF(SUM(COALESCE(wal_buffers_full, 0)), 0) FROM stat WHERE NOT visible " +
		"GROUP BY DATABASE HAVING EXISTS (SELECT 1 FROM stat WHERE NOT visible)"
)

// postgresStatementsCollector ...
type postgresStatementsCollector struct {
	query         typedDesc
	calls         typedDesc
	rows          typedDesc
	times         typedDesc
	allTimes      typedDesc
	sharedHit     typedDesc
	sharedRead    typedDesc
	sharedDirtied typedDesc
	sharedWritten typedDesc
	localHit      typedDesc
	localRead     typedDesc
	localDirtied  typedDesc
	localWritten  typedDesc
	tempRead      typedDesc
	tempWritten   typedDesc
	walRecords    typedDesc
	walBuffers    typedDesc
	walAllBytes   typedDesc
	walBytes      typedDesc
}

// NewPostgresStatementsCollector returns a new Collector exposing postgres statements stats.
// For details see https://www.postgresql.org/docs/current/pgstatstatements.html
func NewPostgresStatementsCollector(constLabels labels, settings model.CollectorSettings) (Collector, error) {
	return &postgresStatementsCollector{
		query: newBuiltinTypedDesc(
			descOpts{"postgres", "statements", "query_info", "Labeled info about statements has been executed.", 0},
			prometheus.GaugeValue,
			[]string{"user", "database", "queryid", "query"}, constLabels,
			settings.Filters,
		),
		calls: newBuiltinTypedDesc(
			descOpts{"postgres", "statements", "calls_total", "Total number of times statement has been executed.", 0},
			prometheus.CounterValue,
			[]string{"user", "database", "queryid"}, constLabels,
			settings.Filters,
		),
		rows: newBuiltinTypedDesc(
			descOpts{"postgres", "statements", "rows_total", "Total number of rows retrieved or affected by the statement.", 0},
			prometheus.CounterValue,
			[]string{"user", "database", "queryid"}, constLabels,
			settings.Filters,
		),
		times: newBuiltinTypedDesc(
			descOpts{"postgres", "statements", "time_seconds_total", "Time spent by the statement in each mode, in seconds.", .001},
			prometheus.CounterValue,
			[]string{"user", "database", "queryid", "mode"}, constLabels,
			settings.Filters,
		),
		allTimes: newBuiltinTypedDesc(
			descOpts{"postgres", "statements", "time_seconds_all_total", "Total time spent by the statement, in seconds.", .001},
			prometheus.CounterValue,
			[]string{"user", "database", "queryid"}, constLabels,
			settings.Filters,
		),
		sharedHit: newBuiltinTypedDesc(
			descOpts{"postgres", "statements", "shared_buffers_hit_total", "Total number of blocks have been found in shared buffers by the statement.", 0},
			prometheus.CounterValue,
			[]string{"user", "database", "queryid"}, constLabels,
			settings.Filters,
		),
		sharedRead: newBuiltinTypedDesc(
			descOpts{"postgres", "statements", "shared_buffers_read_bytes_total", "Total number of bytes read from disk or OS page cache by the statement when block not found in shared buffers.", 0},
			prometheus.CounterValue,
			[]string{"user", "database", "queryid"}, constLabels,
			settings.Filters,
		),
		sharedDirtied: newBuiltinTypedDesc(
			descOpts{"postgres", "statements", "shared_buffers_dirtied_total", "Total number of blocks have been dirtied in shared buffers by the statement.", 0},
			prometheus.CounterValue,
			[]string{"user", "database", "queryid"}, constLabels,
			settings.Filters,
		),
		sharedWritten: newBuiltinTypedDesc(
			descOpts{"postgres", "statements", "shared_buffers_written_bytes_total", "Total number of bytes written from shared buffers to disk by the statement.", 0},
			prometheus.CounterValue,
			[]string{"user", "database", "queryid"}, constLabels,
			settings.Filters,
		),
		localHit: newBuiltinTypedDesc(
			descOpts{"postgres", "statements", "local_buffers_hit_total", "Total number of blocks have been found in local buffers by the statement.", 0},
			prometheus.CounterValue,
			[]string{"user", "database", "queryid"}, constLabels,
			settings.Filters,
		),
		localRead: newBuiltinTypedDesc(
			descOpts{"postgres", "statements", "local_buffers_read_bytes_total", "Total number of bytes read from disk or OS page cache by the statement when block not found in local buffers.", 0},
			prometheus.CounterValue,
			[]string{"user", "database", "queryid"}, constLabels,
			settings.Filters,
		),
		localDirtied: newBuiltinTypedDesc(
			descOpts{"postgres", "statements", "local_buffers_dirtied_total", "Total number of blocks have been dirtied in local buffers by the statement.", 0},
			prometheus.CounterValue,
			[]string{"user", "database", "queryid"}, constLabels,
			settings.Filters,
		),
		localWritten: newBuiltinTypedDesc(
			descOpts{"postgres", "statements", "local_buffers_written_bytes_total", "Total number of bytes written from local buffers to disk by the statement.", 0},
			prometheus.CounterValue,
			[]string{"user", "database", "queryid"}, constLabels,
			settings.Filters,
		),
		tempRead: newBuiltinTypedDesc(
			descOpts{"postgres", "statements", "temp_read_bytes_total", "Total number of bytes read from temporary files by the statement.", 0},
			prometheus.CounterValue,
			[]string{"user", "database", "queryid"}, constLabels,
			settings.Filters,
		),
		tempWritten: newBuiltinTypedDesc(
			descOpts{"postgres", "statements", "temp_written_bytes_total", "Total number of bytes written to temporary files by the statement.", 0},
			prometheus.CounterValue,
			[]string{"user", "database", "queryid"}, constLabels,
			settings.Filters,
		),
		walRecords: newBuiltinTypedDesc(
			descOpts{"postgres", "statements", "wal_records_total", "Total number of WAL records generated by the statement.", 0},
			prometheus.CounterValue,
			[]string{"user", "database", "queryid"}, constLabels,
			settings.Filters,
		),
		walBuffers: newBuiltinTypedDesc(
			descOpts{"postgres", "statements", "wal_buffers_full", "Total number of times the WAL buffers became full generated by the statement.", 0},
			prometheus.CounterValue,
			[]string{"user", "database", "queryid"}, constLabels,
			settings.Filters,
		),
		walAllBytes: newBuiltinTypedDesc(
			descOpts{"postgres", "statements", "wal_bytes_all_total", "Total number of WAL generated by the statement, in bytes.", 0},
			prometheus.CounterValue,
			[]string{"user", "database", "queryid"}, constLabels,
			settings.Filters,
		),
		walBytes: newBuiltinTypedDesc(
			descOpts{"postgres", "statements", "wal_bytes_total", "Total number of WAL bytes generated by the statement, by type.", 0},
			prometheus.CounterValue,
			[]string{"user", "database", "queryid", "wal"}, constLabels,
			settings.Filters,
		),
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *postgresStatementsCollector) Update(ctx context.Context, config Config, ch chan<- prometheus.Metric) error {
	// nothing to do, pg_stat_statements not found in shared_preload_libraries
	if !config.pgStatStatements {
		return nil
	}

	var err error
	conn := config.DB

	var res *model.PGResult
	var cacheKey string
	wg := &sync.WaitGroup{}
	defer wg.Wait()

	// get pg_stat_statements stats
	if config.CollectTopQuery > 0 {
		query := selectStatementsQuery(config.pgVersion.Numeric, config.pgStatStatementsSchema, config.NoTrackMode, config.CollectTopQuery)
		cacheKey, res, _ = getFromCache(config.CacheConfig, config.ConnString, collectorPostgresStatements, query, config.CollectTopQuery)
		if res == nil {
			res, err = conn.Query(ctx, query, config.CollectTopQuery)
			if err != nil {
				return err
			}
			saveToCache(collectorPostgresStatements, wg, config.CacheConfig, cacheKey, res)
		}
	} else {
		query := selectStatementsQuery(config.pgVersion.Numeric, config.pgStatStatementsSchema, config.NoTrackMode, config.CollectTopQuery)
		cacheKey, res, _ = getFromCache(config.CacheConfig, config.ConnString, collectorPostgresStatements, query)
		if res == nil {
			res, err = conn.Query(ctx, query)
			if err != nil {
				return err
			}
			saveToCache(collectorPostgresStatements, wg, config.CacheConfig, cacheKey, res)
		}
	}

	// parse pg_stat_statements stats
	stats := parsePostgresStatementsStats(res, []string{"user", "database", "queryid", "query"})

	blockSize := float64(config.blockSize)

	for _, stat := range stats {
		var query string
		if config.NoTrackMode {
			query = "/* query text hidden, no-track mode enabled */"
		} else {
			query = stat.query
		}

		// Note: pg_stat_statements.total_exec_time (and .total_time) includes blk_read_time and blk_write_time implicitly.
		// Remember that when creating metrics.

		ch <- c.query.newConstMetric(1, stat.user, stat.database, stat.queryid, query)
		ch <- c.calls.newConstMetric(stat.calls, stat.user, stat.database, stat.queryid)
		ch <- c.rows.newConstMetric(stat.rows, stat.user, stat.database, stat.queryid)

		// total = planning + execution; execution already includes io time.
		ch <- c.allTimes.newConstMetric(stat.totalPlanTime+stat.totalExecTime, stat.user, stat.database, stat.queryid)
		ch <- c.times.newConstMetric(stat.totalPlanTime, stat.user, stat.database, stat.queryid, "planning")

		// execution time = execution - io times.
		ch <- c.times.newConstMetric(stat.totalExecTime-(stat.blkReadTime+stat.blkWriteTime), stat.user, stat.database, stat.queryid, "executing")

		// avoid metrics spamming and send metrics only if they greater than zero.
		if stat.blkReadTime > 0 {
			ch <- c.times.newConstMetric(stat.blkReadTime, stat.user, stat.database, stat.queryid, "ioread")
		}
		if stat.blkWriteTime > 0 {
			ch <- c.times.newConstMetric(stat.blkWriteTime, stat.user, stat.database, stat.queryid, "iowrite")
		}
		if stat.sharedBlksHit > 0 {
			ch <- c.sharedHit.newConstMetric(stat.sharedBlksHit, stat.user, stat.database, stat.queryid)
		}
		if stat.sharedBlksRead > 0 {
			ch <- c.sharedRead.newConstMetric(stat.sharedBlksRead*blockSize, stat.user, stat.database, stat.queryid)
		}
		if stat.sharedBlksDirtied > 0 {
			ch <- c.sharedDirtied.newConstMetric(stat.sharedBlksDirtied, stat.user, stat.database, stat.queryid)
		}
		if stat.sharedBlksWritten > 0 {
			ch <- c.sharedWritten.newConstMetric(stat.sharedBlksWritten*blockSize, stat.user, stat.database, stat.queryid)
		}
		if stat.localBlksHit > 0 {
			ch <- c.localHit.newConstMetric(stat.localBlksHit, stat.user, stat.database, stat.queryid)
		}
		if stat.localBlksRead > 0 {
			ch <- c.localRead.newConstMetric(stat.localBlksRead*blockSize, stat.user, stat.database, stat.queryid)
		}
		if stat.localBlksDirtied > 0 {
			ch <- c.localDirtied.newConstMetric(stat.localBlksDirtied, stat.user, stat.database, stat.queryid)
		}
		if stat.localBlksWritten > 0 {
			ch <- c.localWritten.newConstMetric(stat.localBlksWritten*blockSize, stat.user, stat.database, stat.queryid)
		}
		if stat.tempBlksRead > 0 {
			ch <- c.tempRead.newConstMetric(stat.tempBlksRead*blockSize, stat.user, stat.database, stat.queryid)
		}
		if stat.tempBlksWritten > 0 {
			ch <- c.tempWritten.newConstMetric(stat.tempBlksWritten*blockSize, stat.user, stat.database, stat.queryid)
		}
		if stat.walRecords > 0 {
			// WAL records
			ch <- c.walRecords.newConstMetric(stat.walRecords, stat.user, stat.database, stat.queryid)

			// WAL total bytes
			ch <- c.walAllBytes.newConstMetric((stat.walFPI*blockSize)+stat.walBytes, stat.user, stat.database, stat.queryid)

			// WAL bytes by type (regular of fpi)
			ch <- c.walBytes.newConstMetric(stat.walFPI*blockSize, stat.user, stat.database, stat.queryid, "fpi")
			ch <- c.walBytes.newConstMetric(stat.walBytes, stat.user, stat.database, stat.queryid, "regular")

			if config.pgVersion.Numeric >= PostgresV18 {
				// WAL buffers
				ch <- c.walBuffers.newConstMetric(stat.walBuffers, stat.user, stat.database, stat.queryid)
			}
		}
	}

	return nil
}

// postgresStatementStat represents stats values for single statement based on pg_stat_statements.
type postgresStatementStat struct {
	database          string
	user              string
	queryid           string
	query             string
	calls             float64
	rows              float64
	totalExecTime     float64
	totalPlanTime     float64
	blkReadTime       float64
	blkWriteTime      float64
	sharedBlksHit     float64
	sharedBlksRead    float64
	sharedBlksDirtied float64
	sharedBlksWritten float64
	localBlksHit      float64
	localBlksRead     float64
	localBlksDirtied  float64
	localBlksWritten  float64
	tempBlksRead      float64
	tempBlksWritten   float64
	walRecords        float64
	walFPI            float64
	walBytes          float64
	walBuffers        float64
}

// parsePostgresStatementsStats parses PGResult and return structs with stats values.
func parsePostgresStatementsStats(r *model.PGResult, labelNames []string) map[string]postgresStatementStat {
	log.Debug("parse postgres statements stats")

	var stats = make(map[string]postgresStatementStat)

	// process row by row - on every row construct 'statement' using database/user/queryHash trio. Next process other row's
	// fields and collect stats for constructed 'statement'.
	for _, row := range r.Rows {
		var database, user, queryid, query string

		// collect label values
		for i, colname := range r.Colnames {
			switch string(colname.Name) {
			case "database":
				database = row[i].String
			case "user":
				user = row[i].String
			case "queryid":
				queryid = row[i].String
			case "query":
				query = row[i].String
			}
		}

		// Create a statement name consisting of trio database/user/queryHash
		statement := strings.Join([]string{database, user, queryid}, "/")

		// Put stats with labels (but with no data values yet) into stats store.
		if _, ok := stats[statement]; !ok {
			stats[statement] = postgresStatementStat{database: database, user: user, queryid: queryid, query: query}
		}

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

			s := stats[statement]

			// Run column-specific logic
			switch string(colname.Name) {
			case "calls":
				s.calls += v
			case "rows":
				s.rows += v
			case "total_time", "total_exec_time":
				s.totalExecTime += v
			case "total_plan_time":
				s.totalPlanTime += v
			case "blk_read_time":
				s.blkReadTime += v
			case "blk_write_time":
				s.blkWriteTime += v
			case "shared_blks_hit":
				s.sharedBlksHit += v
			case "shared_blks_read":
				s.sharedBlksRead += v
			case "shared_blks_dirtied":
				s.sharedBlksDirtied += v
			case "shared_blks_written":
				s.sharedBlksWritten += v
			case "local_blks_hit":
				s.localBlksHit += v
			case "local_blks_read":
				s.localBlksRead += v
			case "local_blks_dirtied":
				s.localBlksDirtied += v
			case "local_blks_written":
				s.localBlksWritten += v
			case "temp_blks_read":
				s.tempBlksRead += v
			case "temp_blks_written":
				s.tempBlksWritten += v
			case "wal_records":
				s.walRecords += v
			case "wal_fpi":
				s.walFPI += v
			case "wal_bytes":
				s.walBytes += v
			case "wal_buffers_full":
				s.walBuffers += v
			default:
				continue
			}

			stats[statement] = s
		}
	}

	return stats
}

// selectStatementsQuery returns suitable statements query depending on passed version.
func selectStatementsQuery(version int, schema string, notrackmode bool, topK int) string {
	var queryColumm string
	if notrackmode {
		queryColumm = "null"
	} else {
		queryColumm = "p.query"
	}
	if version < PostgresV13 {
		if topK > 0 {
			return fmt.Sprintf(postgresStatementsQuery12TopK, queryColumm, schema)
		}
		return fmt.Sprintf(postgresStatementsQuery12, queryColumm, schema)
	} else if version > PostgresV12 && version < PostgresV17 {
		if topK > 0 {
			return fmt.Sprintf(postgresStatementsQuery16TopK, queryColumm, schema)
		}
		return fmt.Sprintf(postgresStatementsQuery16, queryColumm, schema)
	} else if version > PostgresV16 && version < PostgresV18 {
		if topK > 0 {
			return fmt.Sprintf(postgresStatementsQuery17TopK, queryColumm, schema)
		}
		return fmt.Sprintf(postgresStatementsQuery17, queryColumm, schema)
	}
	if topK > 0 {
		return fmt.Sprintf(postgresStatementsQueryLatestTopK, queryColumm, schema)
	}
	return fmt.Sprintf(postgresStatementsQueryLatest, queryColumm, schema)
}
