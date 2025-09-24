package collector

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"

	"github.com/cherts/pgscv/internal/log"
	"github.com/cherts/pgscv/internal/model"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/shirou/gopsutil/v4/cpu"
)

type cpuCollector struct {
	systicks    float64
	cpu         typedDesc
	cpuAll      typedDesc
	cpuGuest    typedDesc
	physicalCnt typedDesc
	logicalCnt  typedDesc
}

// NewCPUCollector returns a new Collector exposing kernel/system statistics.
func NewCPUCollector(constLabels labels, settings model.CollectorSettings) (Collector, error) {
	cmdOutput, err := exec.Command("getconf", "CLK_TCK").Output()
	if err != nil {
		return nil, fmt.Errorf("determine clock frequency failed: %s", err)
	}

	value := strings.TrimSpace(string(cmdOutput))
	systicks, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid input: parse '%s' failed: %w", value, err)
	}

	c := &cpuCollector{
		systicks: systicks,
		cpu: newBuiltinTypedDesc(
			descOpts{"node", "cpu", "seconds_total", "Seconds the CPUs spent in each mode.", 0},
			prometheus.CounterValue,
			[]string{"mode"}, constLabels,
			settings.Filters,
		),
		cpuAll: newBuiltinTypedDesc(
			descOpts{"node", "cpu", "seconds_all_total", "Seconds the CPUs spent in all modes.", 0},
			prometheus.CounterValue,
			nil, constLabels,
			settings.Filters,
		),
		cpuGuest: newBuiltinTypedDesc(
			descOpts{"node", "cpu", "guest_seconds_total", "Seconds the CPUs spent in guests (VMs) for each mode.", 0},
			prometheus.CounterValue,
			[]string{"mode"}, constLabels,
			settings.Filters,
		),
		physicalCnt: newBuiltinTypedDesc(
			descOpts{"node", "cpu", "physical_core", "Total physical number of core.", 0},
			prometheus.CounterValue,
			nil, constLabels,
			settings.Filters,
		),
		logicalCnt: newBuiltinTypedDesc(
			descOpts{"node", "cpu", "logical_core", "Total logical number of core.", 0},
			prometheus.CounterValue,
			nil, constLabels,
			settings.Filters,
		),
	}
	return c, nil
}

// Update implements Collector and exposes cpu related metrics from /proc/stat and /sys/.../cpu/.
func (c *cpuCollector) Update(_ Config, ch chan<- prometheus.Metric) error {
	cpuStat, err := cpu.Times(false)
	if err != nil {
		return fmt.Errorf("collect cpu usage stats failed: %s; skip", err)
	}

	// Collected time represents summary time spent by ALL cpu cores.
	for _, cpuData := range cpuStat {
		ch <- c.cpu.newConstMetric(cpuData.User, "user")
		ch <- c.cpu.newConstMetric(cpuData.Nice, "nice")
		ch <- c.cpu.newConstMetric(cpuData.System, "system")
		ch <- c.cpu.newConstMetric(cpuData.Idle, "idle")
		ch <- c.cpu.newConstMetric(cpuData.Iowait, "iowait")
		ch <- c.cpu.newConstMetric(cpuData.Irq, "irq")
		ch <- c.cpu.newConstMetric(cpuData.Softirq, "softirq")
		ch <- c.cpu.newConstMetric(cpuData.Steal, "steal")
		ch <- c.cpuAll.newConstMetric(cpuData.User + cpuData.Nice + cpuData.System + cpuData.Idle + cpuData.Iowait + cpuData.Irq + cpuData.Softirq + cpuData.Steal)
		// Guest CPU is also accounted for in stat.user and stat.nice, expose these as separate metrics.
		ch <- c.cpuGuest.newConstMetric(cpuData.Guest, "user")
		ch <- c.cpuGuest.newConstMetric(cpuData.GuestNice, "nice")
	}

	// Get physical and logical CPU core
	physicalCnt, err := cpu.Counts(false)
	if err != nil {
		return err
	}
	if physicalCnt == 0 {
		log.Warnf("could not get physical CPU counts: %v", physicalCnt)
	}
	logicalCnt, err := cpu.Counts(true)
	if err != nil {
		return err
	}
	if logicalCnt == 0 {
		log.Warnf("could not get physical CPU counts: %v", logicalCnt)
	}
	ch <- c.physicalCnt.newConstMetric(float64(physicalCnt))
	ch <- c.logicalCnt.newConstMetric(float64(logicalCnt))

	return nil
}
