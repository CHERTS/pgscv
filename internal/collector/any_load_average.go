// Package collector is a pgSCV collectors
package collector

import (
	"fmt"

	"github.com/cherts/pgscv/internal/model"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/shirou/gopsutil/v4/load"
)

type loadaverageCollector struct {
	load1  typedDesc
	load5  typedDesc
	load15 typedDesc
}

// NewLoadAverageCollector returns a new Collector exposing load average statistics.
func NewLoadAverageCollector(constLabels labels, settings model.CollectorSettings) (Collector, error) {
	return &loadaverageCollector{
		load1: newBuiltinTypedDesc(
			descOpts{"node", "", "load1", "1m load average.", 0},
			prometheus.GaugeValue,
			nil, constLabels,
			settings.Filters,
		),
		load5: newBuiltinTypedDesc(
			descOpts{"node", "", "load5", "5m load average.", 0},
			prometheus.GaugeValue,
			nil, constLabels,
			settings.Filters,
		),
		load15: newBuiltinTypedDesc(
			descOpts{"node", "", "load15", "15m load average.", 0},
			prometheus.GaugeValue,
			nil, constLabels,
			settings.Filters,
		),
	}, nil
}

// Update implements Collector and exposes load average related metrics from /proc/loadavg.
func (c *loadaverageCollector) Update(_ Config, ch chan<- prometheus.Metric) error {
	stats, err := load.Avg()
	if err != nil {
		return fmt.Errorf("failed to get load average stats: %s", err)
	}

	ch <- c.load1.newConstMetric(stats.Load1)
	ch <- c.load5.newConstMetric(stats.Load5)
	ch <- c.load15.newConstMetric(stats.Load15)

	return nil
}
