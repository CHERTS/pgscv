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
	postgresStatSlruQuery = "SELECT name, COALESCE(blks_zeroed, 0) AS blks_zeroed, COALESCE(blks_hit, 0) AS blks_hit, " +
		"COALESCE(blks_read, 0) AS blks_read, COALESCE(blks_written, 0) AS blks_written, COALESCE(blks_exists, 0) AS blks_exists, " +
		"COALESCE(flushes, 0) AS flushes, COALESCE(truncates, 0) AS truncates FROM pg_stat_slru"
)

// postgresStatSlruCollector defines metric descriptors and stats store.
type postgresStatSlruCollector struct {
	blksZeroed  typedDesc
	blksHit     typedDesc
	blksRead    typedDesc
	blksWritten typedDesc
	blksExists  typedDesc
	flushes     typedDesc
	truncates   typedDesc
	labelNames  []string
}

// NewPostgresStatSlruCollector returns a new Collector exposing postgres pg_stat_slru stats.
func NewPostgresStatSlruCollector(constLabels labels, settings model.CollectorSettings) (Collector, error) {
	var labelNames = []string{"name"}

	return &postgresStatSlruCollector{
		labelNames: labelNames,
		blksZeroed: newBuiltinTypedDesc(
			descOpts{"postgres", "stat_slru", "blks_zeroed", "Number of blocks zeroed during initializations.", 0},
			prometheus.GaugeValue,
			labelNames, constLabels,
			settings.Filters,
		),
		blksHit: newBuiltinTypedDesc(
			descOpts{"postgres", "stat_slru", "blks_hit", "Number of times disk blocks were found already in the SLRU, so that a read was not necessary (this only includes hits in the SLRU, not the operating system's file system cache).", 0},
			prometheus.GaugeValue,
			labelNames, constLabels,
			settings.Filters,
		),
		blksRead: newBuiltinTypedDesc(
			descOpts{"postgres", "stat_slru", "blks_read", "Number of disk blocks read for this SLRU.", 0},
			prometheus.GaugeValue,
			labelNames, constLabels,
			settings.Filters,
		),
		blksWritten: newBuiltinTypedDesc(
			descOpts{"postgres", "stat_slru", "blks_written", "Number of disk blocks written for this SLRU.", 0},
			prometheus.GaugeValue,
			labelNames, constLabels,
			settings.Filters,
		),
		blksExists: newBuiltinTypedDesc(
			descOpts{"postgres", "stat_slru", "blks_exists", "Number of blocks checked for existence for this SLRU.", 0},
			prometheus.GaugeValue,
			labelNames, constLabels,
			settings.Filters,
		),
		flushes: newBuiltinTypedDesc(
			descOpts{"postgres", "stat_slru", "flushes", "Number of flushes of dirty data for this SLRU.", 0},
			prometheus.GaugeValue,
			labelNames, constLabels,
			settings.Filters,
		),
		truncates: newBuiltinTypedDesc(
			descOpts{"postgres", "stat_slru", "truncates", "Number of truncates for this SLRU.", 0},
			prometheus.GaugeValue,
			labelNames, constLabels,
			settings.Filters,
		),
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *postgresStatSlruCollector) Update(ctx context.Context, config Config, ch chan<- prometheus.Metric) error {
	if config.pgVersion.Numeric < PostgresV13 {
		log.Debugln("[postgres stat_slru collector]: pg_stat_slru view are not available, required Postgres 13 or newer")
		return nil
	}

	conn := config.DB
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	var err error

	cacheKey, res, _ := getFromCache(config.CacheConfig, config.ConnString, collectorPostgresStatSLRU, postgresStatSlruQuery)
	if res == nil {
		res, err = conn.Query(ctx, postgresStatSlruQuery)
		if err != nil {
			log.Warnf("get pg_stat_slru failed: %s; skip", err)
			return err
		}
		saveToCache(collectorPostgresStatSLRU, wg, config.CacheConfig, cacheKey, res)
	}

	stats := parsePostgresStatSlru(res, c.labelNames)

	for _, stat := range stats {
		ch <- c.blksZeroed.newConstMetric(stat.BlksZeroed, stat.SlruName)
		ch <- c.blksHit.newConstMetric(stat.BlksHit, stat.SlruName)
		ch <- c.blksRead.newConstMetric(stat.BlksRead, stat.SlruName)
		ch <- c.blksWritten.newConstMetric(stat.BlksWritten, stat.SlruName)
		ch <- c.blksExists.newConstMetric(stat.BlksExists, stat.SlruName)
		ch <- c.flushes.newConstMetric(stat.Flushes, stat.SlruName)
		ch <- c.truncates.newConstMetric(stat.Truncates, stat.SlruName)
	}

	return nil
}

// postgresStatSlru
type postgresStatSlru struct {
	SlruName    string // a name of SLRU-cache
	BlksZeroed  float64
	BlksHit     float64
	BlksRead    float64
	BlksWritten float64
	BlksExists  float64
	Flushes     float64
	Truncates   float64
}

// parsePostgresStatSlru parses PGResult and returns structs with stats values.
func parsePostgresStatSlru(r *model.PGResult, labelNames []string) map[string]postgresStatSlru {
	log.Debug("parse postgres stat_slru stats")

	var stats = make(map[string]postgresStatSlru)

	for _, row := range r.Rows {
		var SlruName string

		for i, colname := range r.Colnames {
			switch string(colname.Name) {
			case "name":
				SlruName = row[i].String
			}
		}

		// create a stat_slru name consisting of trio SlruName
		statSlru := strings.Join([]string{SlruName}, "")

		// Put stats with labels (but with no data values yet) into stats store.
		if _, ok := stats[statSlru]; !ok {
			stats[statSlru] = postgresStatSlru{SlruName: SlruName}
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

			s := stats[statSlru]

			switch string(colname.Name) {
			case "blks_zeroed":
				s.BlksZeroed = v
			case "blks_hit":
				s.BlksHit = v
			case "blks_read":
				s.BlksRead = v
			case "blks_written":
				s.BlksWritten = v
			case "blks_exists":
				s.BlksExists = v
			case "flushes":
				s.Flushes = v
			case "truncates":
				s.Truncates = v
			default:
				continue
			}

			stats[statSlru] = s
		}
	}

	return stats
}
