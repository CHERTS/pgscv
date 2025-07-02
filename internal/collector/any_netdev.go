// Package collector is a pgSCV collectors
package collector

import (
	"fmt"

	"github.com/cherts/pgscv/internal/filter"
	"github.com/cherts/pgscv/internal/model"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/shirou/gopsutil/v4/net"
)

type netdevCollector struct {
	bytes   typedDesc
	packets typedDesc
	events  typedDesc
}

// NewNetdevCollector returns a new Collector exposing network interfaces stats.
func NewNetdevCollector(constLabels labels, settings model.CollectorSettings) (Collector, error) {

	// Define default filters (if no already present) to avoid collecting metrics about virtual interfaces.
	if _, ok := settings.Filters["device"]; !ok {
		if settings.Filters == nil {
			settings.Filters = filter.New()
		}

		settings.Filters.Add("device", filter.Filter{Exclude: `docker|virbr`})
		err := settings.Filters.Compile()
		if err != nil {
			return nil, err
		}
	}

	return &netdevCollector{
		bytes: newBuiltinTypedDesc(
			descOpts{"node", "network", "bytes_total", "Total number of bytes processed by network device, by each direction.", 0},
			prometheus.CounterValue,
			[]string{"device", "type"}, constLabels,
			settings.Filters,
		),
		packets: newBuiltinTypedDesc(
			descOpts{"node", "network", "packets_total", "Total number of packets processed by network device, by each direction.", 0},
			prometheus.CounterValue,
			[]string{"device", "type"}, constLabels,
			settings.Filters,
		),
		events: newBuiltinTypedDesc(
			descOpts{"node", "network", "events_total", "Total number of events occurred on network device, by each type and direction.", 0},
			prometheus.CounterValue,
			[]string{"device", "type", "event"}, constLabels,
			settings.Filters,
		),
	}, nil
}

// Update method collects network interfaces statistics
func (c *netdevCollector) Update(_ Config, ch chan<- prometheus.Metric) error {
	netStat, err := net.IOCounters(true)
	if err != nil {
		return fmt.Errorf("Failed to get netdev stats: %s", err)
	}

	for _, stat := range netStat {
		// recv
		ch <- c.bytes.newConstMetric(float64(stat.BytesRecv), stat.Name, "recv")
		ch <- c.packets.newConstMetric(float64(stat.PacketsRecv), stat.Name, "recv")
		ch <- c.events.newConstMetric(float64(stat.Errin), stat.Name, "recv", "errs")
		ch <- c.events.newConstMetric(float64(stat.Dropin), stat.Name, "recv", "drop")
		ch <- c.events.newConstMetric(float64(stat.Fifoin), stat.Name, "recv", "fifo")

		// sent
		ch <- c.bytes.newConstMetric(float64(stat.BytesSent), stat.Name, "sent")
		ch <- c.packets.newConstMetric(float64(stat.PacketsSent), stat.Name, "sent")
		ch <- c.events.newConstMetric(float64(stat.Errout), stat.Name, "sent", "errs")
		ch <- c.events.newConstMetric(float64(stat.Dropout), stat.Name, "sent", "drop")
		ch <- c.events.newConstMetric(float64(stat.Fifoout), stat.Name, "sent", "fifo")
	}

	return nil
}
