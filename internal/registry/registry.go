// Package registry is a pgSCV registry
package registry

import (
	"github.com/cherts/pgscv/internal/collector"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Registry struct
type Registry struct {
	prometheus.Registry
}

// NewRegistry return pointer no Registry
func NewRegistry(_ collector.Factories, _ collector.Config) *Registry {
	r := &Registry{
		*prometheus.NewRegistry(),
	}
	r.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	r.MustRegister(collectors.NewGoCollector())
	return r
}
