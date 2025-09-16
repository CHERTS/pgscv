package collector

import (
	"testing"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/stretchr/testify/assert"
)

func TestCPUCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"node_cpu_seconds_total",
			"node_cpu_seconds_all_total",
			"node_cpu_guest_seconds_total",
			"node_cpu_physical_core",
			"node_cpu_logical_core",
		},
		collector: NewCPUCollector,
	}

	pipeline(t, input)
}

func Test_CpuInfo(t *testing.T) {
	info, err := cpu.Times(false)
	assert.NoError(t, err)
	assert.NotNil(t, info)
}
