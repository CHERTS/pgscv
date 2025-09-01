package collector

import (
	"github.com/cherts/pgscv/internal/model"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/shirou/gopsutil/v4/host"
)

type sysinfoCollector struct {
	platform typedDesc
	os       typedDesc
	host     typedDesc
	uptime   typedDesc
	boottime typedDesc
}

// NewSysInfoCollector returns a new Collector exposing system info.
func NewSysInfoCollector(constLabels labels, settings model.CollectorSettings) (Collector, error) {
	return &sysinfoCollector{
		platform: newBuiltinTypedDesc(
			descOpts{"node", "platform", "info", "Labeled system platform information", 0},
			prometheus.GaugeValue,
			[]string{"virtualization_role", "virtualization_system"}, constLabels,
			settings.Filters,
		),
		os: newBuiltinTypedDesc(
			descOpts{"node", "os", "info", "Labeled operating system information.", 0},
			prometheus.GaugeValue,
			[]string{"type", "name", "family", "version", "kernel_arch", "kernel_version"}, constLabels,
			settings.Filters,
		),
		host: newBuiltinTypedDesc(
			descOpts{"node", "host", "info", "Labeled host information.", 0},
			prometheus.GaugeValue,
			[]string{"hostname", "hostid"}, constLabels,
			settings.Filters,
		),
		uptime: newBuiltinTypedDesc(
			descOpts{"node", "uptime", "seconds", "Total number of seconds the system has been up.", 0},
			prometheus.CounterValue,
			nil, constLabels,
			settings.Filters,
		),
		boottime: newBuiltinTypedDesc(
			descOpts{"node", "boottime", "seconds", "Node boot time, in unixtime.", 0},
			prometheus.GaugeValue,
			nil, constLabels,
			settings.Filters,
		),
	}, nil
}

// Update implements Collector and exposes system info metrics.
func (c *sysinfoCollector) Update(_ Config, ch chan<- prometheus.Metric) error {
	info, err := host.Info()
	if err != nil {
		return err
	}

	ch <- c.platform.newConstMetric(1, info.VirtualizationRole, info.VirtualizationSystem)
	ch <- c.os.newConstMetric(1, info.OS, info.Platform, info.PlatformFamily, info.PlatformVersion, info.KernelArch, info.KernelVersion)
	ch <- c.host.newConstMetric(1, info.Hostname, info.HostID)

	ch <- c.uptime.newConstMetric(float64(info.Uptime))
	ch <- c.boottime.newConstMetric(float64(info.BootTime))

	return nil
}
