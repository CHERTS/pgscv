package collector

import (
	"testing"

	"github.com/shirou/gopsutil/v4/host"
	"github.com/stretchr/testify/assert"
)

func TestSysInfoCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"node_platform_info", "node_os_info",
			"node_host_info", "node_boottime_seconds",
			"node_uptime_seconds",
		},
		optional:  []string{},
		collector: NewSysInfoCollector,
	}

	pipeline(t, input)
}

func Test_getSysInfo(t *testing.T) {
	info, err := host.Info()
	assert.NoError(t, err)
	assert.NotNil(t, info)
}
