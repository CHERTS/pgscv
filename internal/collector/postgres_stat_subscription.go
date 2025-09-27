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
	postgresStatSubscriptionQuery14 = "SELECT subid, subname, COALESCE(pid, 0) AS pid, " +
		"COALESCE(NULL::text, 'unknown') AS worker_type, " +
		"COALESCE(received_lsn - '0/0', 0) AS received_lsn, " +
		"COALESCE(latest_end_lsn - '0/0', 0) AS reported_lsn, " +
		"COALESCE(EXTRACT(EPOCH FROM last_msg_send_time), 0) AS msg_send_time," +
		"COALESCE(EXTRACT(EPOCH FROM last_msg_receipt_time), 0) AS msg_recv_time, " +
		"COALESCE(EXTRACT(EPOCH FROM latest_end_time), 0) AS reported_time, " +
		"COALESCE(NULL::numeric, 0) AS apply_error_count, COALESCE(NULL::numeric, 0) AS sync_error_count " +
		"FROM pg_stat_subscription WHERE relid ISNULL;"

	postgresStatSubscriptionQuery16 = "SELECT s1.subid, s1.subname, COALESCE(s1.pid, 0) AS pid, " +
		"COALESCE(NULL::text, 'unknown') AS worker_type, " +
		"COALESCE(received_lsn - '0/0', 0) AS received_lsn, " +
		"COALESCE(latest_end_lsn - '0/0', 0) AS reported_lsn, " +
		"COALESCE(EXTRACT(EPOCH FROM last_msg_send_time), 0) AS msg_send_time," +
		"COALESCE(EXTRACT(EPOCH FROM last_msg_receipt_time), 0) AS msg_recv_time, " +
		"COALESCE(EXTRACT(EPOCH FROM latest_end_time), 0) AS reported_time, " +
		"s2.apply_error_count, s2.sync_error_count " +
		"FROM pg_stat_subscription s1 JOIN pg_stat_subscription_stats s2 ON s1.subid = s2.subid " +
		"WHERE s1.relid ISNULL;"

	postgresStatSubscriptionQuery17 = "SELECT s1.subid, s1.subname, COALESCE(s1.pid, 0) AS pid, " +
		"COALESCE(s1.worker_type, 'unknown') AS worker_type, " +
		"COALESCE(received_lsn - '0/0', 0) AS received_lsn, " +
		"COALESCE(latest_end_lsn - '0/0', 0) AS reported_lsn, " +
		"COALESCE(EXTRACT(EPOCH FROM last_msg_send_time), 0) AS msg_send_time, " +
		"COALESCE(EXTRACT(EPOCH FROM last_msg_receipt_time), 0) AS msg_recv_time, " +
		"COALESCE(EXTRACT(EPOCH FROM latest_end_time), 0) AS reported_time, " +
		"s2.apply_error_count, s2.sync_error_count " +
		"FROM pg_stat_subscription s1 JOIN pg_stat_subscription_stats s2 ON s1.subid = s2.subid " +
		"WHERE s1.relid ISNULL;"

	postgresStatSubscriptionQueryLatest = "SELECT s1.subid, s1.subname, COALESCE(s1.pid, 0) AS pid, " +
		"COALESCE(s1.worker_type, 'unknown') AS worker_type, " +
		"COALESCE(received_lsn - '0/0', 0) AS received_lsn, " +
		"COALESCE(latest_end_lsn - '0/0', 0) AS reported_lsn, " +
		"COALESCE(EXTRACT(EPOCH FROM last_msg_send_time), 0) AS msg_send_time, " +
		"COALESCE(EXTRACT(EPOCH FROM last_msg_receipt_time), 0) AS msg_recv_time, " +
		"COALESCE(EXTRACT(EPOCH FROM latest_end_time), 0) AS reported_time, " +
		"s2.apply_error_count, s2.sync_error_count, " +
		"s2.confl_insert_exists, s2.confl_update_origin_differs, " +
		"s2.confl_update_exists, s2.confl_update_missing, " +
		"s2.confl_delete_origin_differs, s2.confl_delete_missing, " +
		"s2.confl_multiple_unique_conflicts " +
		"FROM pg_stat_subscription s1 JOIN pg_stat_subscription_stats s2 ON s1.subid = s2.subid " +
		"WHERE s1.relid ISNULL;"
)

// postgresStatSubscriptionCollector defines metric descriptors and stats store.
type postgresStatSubscriptionCollector struct {
	labelNames   []string
	receivedLsn  typedDesc
	reportedLsn  typedDesc
	msgSendtime  typedDesc
	msgRecvtime  typedDesc
	reportedTime typedDesc
	errorCount   typedDesc
	conflCount   typedDesc
}

// NewPostgresStatSubscriptionCollector returns a new Collector exposing postgres pg_stat_subscription stats.
// For details see https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-SUBSCRIPTION
func NewPostgresStatSubscriptionCollector(constLabels labels, settings model.CollectorSettings) (Collector, error) {
	var labelNames = []string{"subid", "subname", "worker_type"}

	return &postgresStatSubscriptionCollector{
		labelNames: labelNames,
		receivedLsn: newBuiltinTypedDesc(
			descOpts{"postgres", "stat_subscription", "received_lsn", "Last write-ahead log location received.", 0},
			prometheus.GaugeValue,
			labelNames, constLabels,
			settings.Filters,
		),
		reportedLsn: newBuiltinTypedDesc(
			descOpts{"postgres", "stat_subscription", "reported_lsn", "Last write-ahead log location reported to origin WAL sender.", 0},
			prometheus.GaugeValue,
			labelNames, constLabels,
			settings.Filters,
		),
		msgSendtime: newBuiltinTypedDesc(
			descOpts{"postgres", "stat_subscription", "msg_send_time", "Send time of last message received from origin WAL sender.", 0},
			prometheus.GaugeValue,
			labelNames, constLabels,
			settings.Filters,
		),
		msgRecvtime: newBuiltinTypedDesc(
			descOpts{"postgres", "stat_subscription", "msg_recv_time", "Receipt time of last message received from origin WAL sender.", 0},
			prometheus.GaugeValue,
			labelNames, constLabels,
			settings.Filters,
		),
		reportedTime: newBuiltinTypedDesc(
			descOpts{"postgres", "stat_subscription", "reported_time", "Time of last write-ahead log location reported to origin WAL sender.", 0},
			prometheus.GaugeValue,
			labelNames, constLabels,
			settings.Filters,
		),
		errorCount: newBuiltinTypedDesc(
			descOpts{"postgres", "stat_subscription", "error_count", "Number of times an error occurred (applying changes OR initial table synchronization).", 0},
			prometheus.CounterValue,
			[]string{"subid", "subname", "worker_type", "type"}, constLabels,
			settings.Filters,
		),
		conflCount: newBuiltinTypedDesc(
			descOpts{"postgres", "stat_subscription", "confl_count", "Number of times an additional conflict error occurred.", 0},
			prometheus.CounterValue,
			[]string{"subid", "subname", "worker_type", "type"}, constLabels,
			settings.Filters,
		),
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *postgresStatSubscriptionCollector) Update(ctx context.Context, config Config, ch chan<- prometheus.Metric) error {
	if config.pgVersion.Numeric < PostgresV10 {
		log.Debugln("[postgres stat_subscription collector]: pg_stat_subscription view are not available, required Postgres 10 or newer")
		return nil
	}
	// Collecting pg_stat_subscription since Postgres 10.

	conn := config.DB

	wg := &sync.WaitGroup{}
	defer wg.Wait()
	var err error

	query := selectSubscriptionQuery(config.pgVersion.Numeric)
	cacheKey, res, _ := getFromCache(config.CacheConfig, config.ConnString, collectorPostgresStatSubscription, query)
	if res == nil {
		res, err = conn.Query(ctx, query)
		if err != nil {
			log.Warnf("get pg_stat_subscription failed: %s; skip", err)
			return err
		}
		saveToCache(collectorPostgresStatSubscription, wg, config.CacheConfig, cacheKey, res)
	}
	// Parse pg_stat_subscription stats.
	stats := parsePostgresSubscriptionStat(res, c.labelNames)
	for _, stat := range stats {
		if value, ok := stat.values["received_lsn"]; ok {
			ch <- c.receivedLsn.newConstMetric(value, stat.SubID, stat.SubName, stat.WorkerType)
		}
		if value, ok := stat.values["reported_lsn"]; ok {
			ch <- c.reportedLsn.newConstMetric(value, stat.SubID, stat.SubName, stat.WorkerType)
		}
		if value, ok := stat.values["msg_send_time"]; ok {
			ch <- c.msgSendtime.newConstMetric(value, stat.SubID, stat.SubName, stat.WorkerType)
		}
		if value, ok := stat.values["msg_recv_time"]; ok {
			ch <- c.msgRecvtime.newConstMetric(value, stat.SubID, stat.SubName, stat.WorkerType)
		}
		if value, ok := stat.values["reported_time"]; ok {
			ch <- c.reportedTime.newConstMetric(value, stat.SubID, stat.SubName, stat.WorkerType)
		}
		if value, ok := stat.values["apply_error_count"]; ok {
			ch <- c.errorCount.newConstMetric(value, stat.SubID, stat.SubName, stat.WorkerType, "apply")
		}
		if value, ok := stat.values["sync_error_count"]; ok {
			ch <- c.errorCount.newConstMetric(value, stat.SubID, stat.SubName, stat.WorkerType, "sync")
		}
		if value, ok := stat.values["confl_insert_exists"]; ok {
			ch <- c.conflCount.newConstMetric(value, stat.SubID, stat.SubName, stat.WorkerType, "insert_exists")
		}
		if value, ok := stat.values["confl_update_origin_differs"]; ok {
			ch <- c.conflCount.newConstMetric(value, stat.SubID, stat.SubName, stat.WorkerType, "update_origin_differs")
		}
		if value, ok := stat.values["confl_update_exists"]; ok {
			ch <- c.conflCount.newConstMetric(value, stat.SubID, stat.SubName, stat.WorkerType, "update_exists")
		}
		if value, ok := stat.values["confl_update_missing"]; ok {
			ch <- c.conflCount.newConstMetric(value, stat.SubID, stat.SubName, stat.WorkerType, "update_missing")
		}
		if value, ok := stat.values["confl_delete_origin_differs"]; ok {
			ch <- c.conflCount.newConstMetric(value, stat.SubID, stat.SubName, stat.WorkerType, "delete_origin_differs")
		}
		if value, ok := stat.values["confl_delete_missing"]; ok {
			ch <- c.conflCount.newConstMetric(value, stat.SubID, stat.SubName, stat.WorkerType, "delete_missing")
		}
		if value, ok := stat.values["confl_multiple_unique_conflicts"]; ok {
			ch <- c.conflCount.newConstMetric(value, stat.SubID, stat.SubName, stat.WorkerType, "multiple_unique_conflicts")
		}
	}

	return nil
}

// postgresSubscriptionStat represents per-subscription stats based on pg_stat_subscription.
type postgresSubscriptionStat struct {
	SubID      string // a subscription id
	SubName    string // a subscription name
	Pid        string // a pid
	WorkerType string // a worker_type
	values     map[string]float64
}

// parsePostgresSubscriptionStat parses PGResult and returns structs with stats values.
func parsePostgresSubscriptionStat(r *model.PGResult, labelNames []string) map[string]postgresSubscriptionStat {
	log.Debug("parse postgres stat_subscription stats")

	var stats = make(map[string]postgresSubscriptionStat)

	for _, row := range r.Rows {
		stat := postgresSubscriptionStat{values: map[string]float64{}}

		// collect label values
		for i, colname := range r.Colnames {
			switch string(colname.Name) {
			case "pid":
				stat.Pid = row[i].String
			case "subname":
				stat.SubName = row[i].String
			case "subid":
				stat.SubID = row[i].String
			case "worker_type":
				stat.WorkerType = row[i].String
			}
		}

		// use pid as key in the map
		pid := stat.Pid

		// Put stats with labels (but with no data values yet) into stats store.
		stats[pid] = stat

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

			s := stats[pid]

			// Run column-specific logic
			switch string(colname.Name) {
			case "received_lsn":
				s.values["received_lsn"] = v
			case "reported_lsn":
				s.values["reported_lsn"] = v
			case "msg_send_time":
				s.values["msg_send_time"] = v
			case "msg_recv_time":
				s.values["msg_recv_time"] = v
			case "reported_time":
				s.values["reported_time"] = v
			case "apply_error_count":
				s.values["apply_error_count"] = v
			case "sync_error_count":
				s.values["sync_error_count"] = v
			case "confl_insert_exists":
				s.values["confl_insert_exists"] = v
			case "confl_update_origin_differs":
				s.values["confl_update_origin_differs"] = v
			case "confl_update_exists":
				s.values["confl_update_exists"] = v
			case "confl_update_missing":
				s.values["confl_update_missing"] = v
			case "confl_delete_origin_differs":
				s.values["confl_delete_origin_differs"] = v
			case "confl_delete_missing":
				s.values["confl_delete_missing"] = v
			case "confl_multiple_unique_conflicts":
				s.values["confl_multiple_unique_conflicts"] = v
			default:
				continue
			}

			stats[pid] = s
		}
	}

	return stats
}

// selectSubscriptionQuery returns suitable subscription query depending on passed version.
func selectSubscriptionQuery(version int) string {
	switch {
	case version < PostgresV15:
		return postgresStatSubscriptionQuery14
	case version < PostgresV17:
		return postgresStatSubscriptionQuery16
	case version < PostgresV18:
		return postgresStatSubscriptionQuery17
	default:
		return postgresStatSubscriptionQueryLatest
	}
}
