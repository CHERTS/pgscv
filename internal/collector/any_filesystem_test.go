package collector

import (
	"runtime"
	"testing"

	"github.com/cherts/pgscv/internal/filter"
	"github.com/cherts/pgscv/internal/model"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/stretchr/testify/assert"
)

func TestFilesystemCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"node_filesystem_bytes",
			"node_filesystem_bytes_total",
			"node_filesystem_files",
			"node_filesystem_files_total",
		},
		collector:         NewFilesystemCollector,
		collectorSettings: model.CollectorSettings{Filters: filter.New()},
	}

	pipeline(t, input)
}

func Test_getFilesystemStats(t *testing.T) {
	path := "/"
	if runtime.GOOS == "windows" {
		path = "C:"
	}
	got, err := disk.Usage(path)
	assert.NoError(t, err)
	assert.NotNil(t, got)
	assert.Equalf(t, got.Path, path, "error %v", err)
}
