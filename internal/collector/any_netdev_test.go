package collector

import (
	"testing"

	"github.com/cherts/pgscv/internal/filter"
	"github.com/cherts/pgscv/internal/model"
	"github.com/shirou/gopsutil/v4/net"
	"github.com/stretchr/testify/assert"
)

func TestNetdevCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"node_network_bytes_total",
			"node_network_packets_total",
			"node_network_events_total",
		},
		collector:         NewNetdevCollector,
		collectorSettings: model.CollectorSettings{Filters: filter.New()},
	}

	pipeline(t, input)
}

func Test_getNetdevStats(t *testing.T) {
	info, err := net.IOCounters(true)
	assert.NoError(t, err)
	assert.NotNil(t, info)
}
