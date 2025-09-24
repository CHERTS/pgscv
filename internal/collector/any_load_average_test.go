package collector

import (
	"testing"

	"github.com/shirou/gopsutil/v4/load"
	"github.com/stretchr/testify/assert"
)

func TestLoadAverageCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"node_load1",
			"node_load5",
			"node_load15",
		},
		collector: NewLoadAverageCollector,
	}

	pipeline(t, input)
}

func Test_getLaInfo(t *testing.T) {
	info, err := load.Avg()
	assert.NoError(t, err)
	assert.NotNil(t, info)
}
