// Package collector is a pgSCV collectors
package collector

import (
	"fmt"
	"runtime"

	"github.com/cherts/pgscv/internal/filter"
	"github.com/cherts/pgscv/internal/model"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/shirou/gopsutil/v4/disk"
)

type filesystemCollector struct {
	bytes      typedDesc
	bytesTotal typedDesc
	files      typedDesc
	filesTotal typedDesc
}

// NewFilesystemCollector returns a new Collector exposing filesystem stats.
func NewFilesystemCollector(constLabels labels, settings model.CollectorSettings) (Collector, error) {

	// Define default filters (if no already present) to avoid collecting metrics about exotic filesystems.
	if _, ok := settings.Filters["fstype"]; !ok {
		if settings.Filters == nil {
			settings.Filters = filter.New()
		}
		switch runtime.GOOS {
		case "windows":
			settings.Filters.Add("fstype", filter.Filter{Include: `^(NTFS|FAT32|exFAT)$`})
		case "darwin":
			settings.Filters.Add("fstype", filter.Filter{Include: `^(apfs)$`})
		case "linux":
			settings.Filters.Add("fstype", filter.Filter{Include: `^(ext3|ext4|xfs|btrfs)$`})
		case "freebsd", "openbsd":
			settings.Filters.Add("fstype", filter.Filter{Include: `^(ufs|ufs2)$`})
		}
		err := settings.Filters.Compile()
		if err != nil {
			return nil, err
		}
	}

	return &filesystemCollector{
		bytes: newBuiltinTypedDesc(
			descOpts{"node", "filesystem", "bytes", "Number of bytes of filesystem by usage.", 0},
			prometheus.GaugeValue,
			[]string{"device", "mountpoint", "fstype", "usage"}, constLabels,
			settings.Filters,
		),
		bytesTotal: newBuiltinTypedDesc(
			descOpts{"node", "filesystem", "bytes_total", "Total number of bytes of filesystem capacity.", 0},
			prometheus.GaugeValue,
			[]string{"device", "mountpoint", "fstype"}, constLabels,
			settings.Filters,
		),
		files: newBuiltinTypedDesc(
			descOpts{"node", "filesystem", "files", "Number of files (inodes) of filesystem by usage.", 0},
			prometheus.GaugeValue,
			[]string{"device", "mountpoint", "fstype", "usage"}, constLabels,
			settings.Filters,
		),
		filesTotal: newBuiltinTypedDesc(
			descOpts{"node", "filesystem", "files_total", "Total number of files (inodes) of filesystem capacity.", 0},
			prometheus.GaugeValue,
			[]string{"device", "mountpoint", "fstype"}, constLabels,
			settings.Filters,
		),
	}, nil
}

// Update method collects filesystem usage statistics.
func (c *filesystemCollector) Update(_ Config, ch chan<- prometheus.Metric) error {
	diskStat, err := disk.Partitions(false)
	if err != nil {
		return fmt.Errorf("get filesystem stats failed: %s", err)
	}

	for _, diskData := range diskStat {

		partitionStat, err := disk.Usage(diskData.Mountpoint)
		if err != nil {
			return fmt.Errorf("get partition stats failed: %s", err)
		}

		// Truncate device paths to device names, e.g /dev/sda -> sda
		device := truncateDeviceName(diskData.Device)

		// bytes; free = avail + reserved; total = used + free
		ch <- c.bytesTotal.newConstMetric(float64(partitionStat.Total), device, diskData.Mountpoint, diskData.Fstype)
		ch <- c.bytes.newConstMetric(float64(partitionStat.Total)-float64(partitionStat.Used), device, diskData.Mountpoint, diskData.Fstype, "avail")
		//ch <- c.bytes.newConstMetric(s.free-s.avail, device, diskData.Mountpoint, diskData.Fstype, "reserved")
		ch <- c.bytes.newConstMetric(float64(partitionStat.Used), device, diskData.Mountpoint, diskData.Fstype, "used")
		// files (inodes)
		ch <- c.filesTotal.newConstMetric(float64(partitionStat.InodesTotal), device, diskData.Mountpoint, diskData.Fstype)
		ch <- c.files.newConstMetric(float64(partitionStat.InodesFree), device, diskData.Mountpoint, diskData.Fstype, "free")
		ch <- c.files.newConstMetric(float64(partitionStat.InodesUsed), device, diskData.Mountpoint, diskData.Fstype, "used")
	}

	return nil
}
