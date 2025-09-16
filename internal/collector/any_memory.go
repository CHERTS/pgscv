// Package collector is a pgSCV collectors
package collector

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"

	"github.com/cherts/pgscv/internal/filter"
	"github.com/cherts/pgscv/internal/log"
	"github.com/cherts/pgscv/internal/model"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/shirou/gopsutil/v4/mem"
)

type meminfoCollector struct {
	re            *regexp.Regexp
	subsysFilters filter.Filters
	constLabels   labels
	swapused      typedDesc
}

// NewMeminfoCollector returns a new Collector exposing memory stats.
func NewMeminfoCollector(constLabels labels, settings model.CollectorSettings) (Collector, error) {
	return &meminfoCollector{
		re:            regexp.MustCompile(`\((.*)\)`),
		subsysFilters: settings.Filters,
		constLabels:   constLabels,
		swapused: newBuiltinTypedDesc(
			descOpts{"node", "memory", "SwapUsed", "Memory information composite field SwapUsed.", 0},
			prometheus.GaugeValue,
			nil, constLabels,
			settings.Filters,
		),
	}, nil
}

// Update method collects network interfaces statistics.
func (c *meminfoCollector) Update(_ Config, ch chan<- prometheus.Metric) error {
	memInfo, err := mem.VirtualMemory()
	if err != nil {
		return fmt.Errorf("failed to get VirtualMemory: %s", err)
	}

	memInfoData, err := json.MarshalIndent(memInfo, "", "")
	if err != nil {
		return fmt.Errorf("failed convert VirtualMemory data: %s", err)
	}

	var memInfoJSONMap map[string]any
	if err := json.Unmarshal([]byte(memInfoData), &memInfoJSONMap); err != nil {
		return fmt.Errorf("failed to read VirtualMemory json data: %s", err)
	}

	// Processing memInfoJSONMap stats.
	for param, value := range memInfoJSONMap {
		param = c.re.ReplaceAllString(param, "_${1}")
		desc := newBuiltinTypedDesc(
			descOpts{"node", "memory", param, fmt.Sprintf("Memory information field %s.", param), 0},
			prometheus.GaugeValue,
			nil, c.constLabels,
			c.subsysFilters,
		)

		ch <- desc.newConstMetric(value.(float64))
	}

	swapInfo, err := mem.SwapMemory()
	if err != nil {
		return fmt.Errorf("failed get SwapMemory: %s", err)
	}

	// MemUsed and SwapUsed are composite metrics and not present in /proc/meminfo.
	ch <- c.swapused.newConstMetric(float64(swapInfo.Total) - float64(swapInfo.Free))

	if runtime.GOOS == "linux" {
		vmstat, err := getVmstatStats()
		if err != nil {
			return fmt.Errorf("get /proc/vmstat stats failed: %s", err)
		}

		// Processing vmstat stats.
		for param, value := range vmstat {
			// Depending on key name, make an assumption about metric type.
			// Analyzing of vmstat content shows that gauge values have 'nr_' prefix. But without of
			// strong knowledge of kernel internals this is just an assumption and could be mistaken.
			t := prometheus.CounterValue
			if strings.HasPrefix(param, "nr_") {
				t = prometheus.GaugeValue
			}

			param = c.re.ReplaceAllString(param, "_${1}")

			desc := newBuiltinTypedDesc(
				descOpts{"node", "vmstat", param, fmt.Sprintf("Vmstat information field %s.", param), 0},
				t, nil, c.constLabels, c.subsysFilters,
			)

			ch <- desc.newConstMetric(value)
		}
	}

	return nil
}

// getVmstatStats is the intermediate function which opens stats file and run stats parser for extracting stats.
func getVmstatStats() (map[string]float64, error) {
	file, err := os.Open("/proc/vmstat")
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()

	return parseVmstatStats(file)
}

// parseVmstatStats accepts file descriptor, reads file content and produces stats.
func parseVmstatStats(r io.Reader) (map[string]float64, error) {
	log.Debug("parse vmstat stats")

	var (
		scanner = bufio.NewScanner(r)
		stats   = map[string]float64{}
	)

	// Parse line by line, split line to param and value, parse the value to float and save to store.
	for scanner.Scan() {
		parts := strings.Fields(scanner.Text())

		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid input, '%s': wrong number of values", scanner.Text())
		}

		param, value := parts[0], parts[1]

		v, err := strconv.ParseFloat(value, 64)
		if err != nil {
			log.Errorf("invalid input, parse '%s' failed: %s, skip", value, err.Error())
			continue
		}

		stats[param] = v
	}

	return stats, scanner.Err()
}
