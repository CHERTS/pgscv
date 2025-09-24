// Package collector is a pgSCV collectors
package collector

import (
	"maps"
	"strconv"
	"sync"

	"github.com/cherts/pgscv/internal/filter"
	"github.com/cherts/pgscv/internal/log"
	"github.com/cherts/pgscv/internal/model"
	"github.com/jackc/pgx/v4"
	"github.com/prometheus/client_golang/prometheus"
)

// Factories defines collector functions which used for collecting metrics.
type Factories map[string]func(labels, model.CollectorSettings) (Collector, error)

// RegisterSystemCollectors unions all system-related collectors and registers them in single place.
func (f Factories) RegisterSystemCollectors(disabled []string) {
	if stringsContains(disabled, "system") {
		log.Debugln("disable all system collectors")
		return
	}

	funcs := map[string]func(labels, model.CollectorSettings) (Collector, error){
		"system/pgscv":       NewPgscvServicesCollector,
		"system/sysinfo":     NewSysInfoCollector,
		"system/loadaverage": NewLoadAverageCollector,
		"system/cpu":         NewCPUCollector,
		"system/diskstats":   NewDiskstatsCollector,
		"system/filesystems": NewFilesystemCollector,
		"system/netdev":      NewNetdevCollector,
		"system/network":     NewNetworkCollector,
		"system/memory":      NewMeminfoCollector,
		"system/sysconfig":   NewSysconfigCollector,
	}

	for name, fn := range funcs {
		if stringsContains(disabled, name) {
			log.Debugln("disable ", name)
			continue
		}

		log.Debugln("enable ", name)
		f.register(name, fn)
	}
}

// RegisterPostgresCollectors unions all postgres-related collectors and registers them in single place.
func (f Factories) RegisterPostgresCollectors(disabled []string) {
	if stringsContains(disabled, "postgres") {
		log.Debugln("disable all postgres collectors")
		return
	}

	funcs := map[string]func(labels, model.CollectorSettings) (Collector, error){
		"postgres/pgscv":             NewPgscvServicesCollector,
		"postgres/activity":          NewPostgresActivityCollector,
		"postgres/archiver":          NewPostgresWalArchivingCollector,
		"postgres/bgwriter":          NewPostgresBgwriterCollector,
		"postgres/conflicts":         NewPostgresConflictsCollector,
		"postgres/databases":         NewPostgresDatabasesCollector,
		"postgres/indexes":           NewPostgresIndexesCollector,
		"postgres/functions":         NewPostgresFunctionsCollector,
		"postgres/locks":             NewPostgresLocksCollector,
		"postgres/logs":              NewPostgresLogsCollector,
		"postgres/replication":       NewPostgresReplicationCollector,
		"postgres/replication_slots": NewPostgresReplicationSlotsCollector,
		"postgres/statements":        NewPostgresStatementsCollector,
		"postgres/schemas":           NewPostgresSchemasCollector,
		"postgres/settings":          NewPostgresSettingsCollector,
		"postgres/storage":           NewPostgresStorageCollector,
		"postgres/stat_io":           NewPostgresStatIOCollector,
		"postgres/stat_slru":         NewPostgresStatSlruCollector,
		"postgres/stat_subscription": NewPostgresStatSubscriptionCollector,
		"postgres/stat_ssl":          NewPostgresStatSslCollector,
		"postgres/tables":            NewPostgresTablesCollector,
		"postgres/wal":               NewPostgresWalCollector,
		"postgres/custom":            NewPostgresCustomCollector,
	}

	for name, fn := range funcs {
		if stringsContains(disabled, name) {
			log.Debugln("disable ", name)
			continue
		}
		log.Debugln("enable ", name)
		f.register(name, fn)
	}
}

// RegisterPgbouncerCollectors unions all pgbouncer-related collectors and registers them in single place.
func (f Factories) RegisterPgbouncerCollectors(disabled []string) {
	if stringsContains(disabled, "pgbouncer") {
		log.Debugln("disable all pgbouncer collectors")
		return
	}

	funcs := map[string]func(labels, model.CollectorSettings) (Collector, error){
		"pgbouncer/pgscv":    NewPgscvServicesCollector,
		"pgbouncer/pools":    NewPgbouncerPoolsCollector,
		"pgbouncer/stats":    NewPgbouncerStatsCollector,
		"pgbouncer/settings": NewPgbouncerSettingsCollector,
	}

	for name, fn := range funcs {
		if stringsContains(disabled, name) {
			log.Debugln("disable ", name)
			continue
		}

		log.Debugln("enable ", name)
		f.register(name, fn)
	}
}

// RegisterPatroniCollectors unions all patroni-related collectors and registers them in single place.
func (f Factories) RegisterPatroniCollectors(disabled []string) {
	if stringsContains(disabled, "patroni") {
		log.Debugln("disable all patroni collectors")
		return
	}

	funcs := map[string]func(labels, model.CollectorSettings) (Collector, error){
		"patroni/pgscv":  NewPgscvServicesCollector,
		"patroni/common": NewPatroniCommonCollector,
	}

	for name, fn := range funcs {
		if stringsContains(disabled, name) {
			log.Debugln("disable ", name)
			continue
		}

		log.Debugln("enable ", name)
		f.register(name, fn)
	}
}

// register is the generic routine which register any kind of collectors.
func (f Factories) register(collector string, factory func(labels, model.CollectorSettings) (Collector, error)) {
	f[collector] = factory
}

// Collector is the interface a collector has to implement.
type Collector interface {
	// Update does collecting new metrics and expose them via prometheus registry.
	Update(config Config, ch chan<- prometheus.Metric) error
}

// PgscvCollector implements the prometheus.Collector interface.
type PgscvCollector struct {
	Config     Config
	Collectors map[string]Collector
	// anchorDesc is a metric descriptor used for distinguishing collectors when unregister is required.
	anchorDesc typedDesc
}

// NewPgscvCollector accepts Factories and creates per-service instance of Collector.
func NewPgscvCollector(serviceID string, factories Factories, config Config) (*PgscvCollector, error) {
	collectors := make(map[string]Collector)

	constLabels := make(labels)
	constLabels["service_id"] = serviceID

	if config.ServiceType == model.ServiceTypePostgresql || config.ServiceType == model.ServiceTypePgbouncer {
		pgConfig, err := pgx.ParseConfig(config.ConnString)
		if err != nil {
			return nil, err
		}
		constLabels["host"] = pgConfig.Host
		constLabels["port"] = strconv.FormatUint(uint64(pgConfig.Port), 10)
	}

	if config.ConstLabels != nil {
		maps.Copy(constLabels, *config.ConstLabels)
	}

	for key := range factories {
		settings := config.Settings[key]

		collector, err := factories[key](constLabels, settings)
		if err != nil {
			return nil, err
		}
		collectors[key] = collector
	}

	// anchorDesc is a metric descriptor used for distinguish collectors. Creating many collectors with uniq anchorDesc makes
	// possible to unregister collectors if they or their associated services become unnecessary or unavailable.
	desc := newBuiltinTypedDesc(
		descOpts{"pgscv", "service", serviceID, "Service metric.", 0},
		prometheus.GaugeValue,
		nil, constLabels,
		filter.New(),
	)

	return &PgscvCollector{Config: config, Collectors: collectors, anchorDesc: desc}, nil
}

// Describe implements the prometheus.Collector interface.
func (n PgscvCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- n.anchorDesc.desc
}

// Collect implements the prometheus.Collector interface.
func (n PgscvCollector) Collect(out chan<- prometheus.Metric) {
	// Update settings of Postgres collectors if service was unavailabled when register
	var concurrencyLimit int
	if n.Config.ServiceType == "postgres" {
		if n.Config.postgresServiceConfig.blockSize == 0 {
			log.Debug("updating service configuration...")
			err := n.Config.FillPostgresServiceConfig(n.Config.ConnTimeout)
			if err != nil {
				log.Errorf("update service config failed: %s", err.Error())
			}
		}
		if n.Config.ConcurrencyLimit != nil {
			log.Debugf("user rolConnLimit: %d", n.Config.rolConnLimit)
			log.Debugf("current ConcurrencyLimit: %d connection limit set for DB", *n.Config.ConcurrencyLimit)
			if n.Config.rolConnLimit > -1 {
				concurrencyLimit = n.Config.rolConnLimit
			} else {
				concurrencyLimit = len(n.Collectors)
			}
			if *n.Config.ConcurrencyLimit < concurrencyLimit {
				concurrencyLimit = *n.Config.ConcurrencyLimit
			}
		} else {
			concurrencyLimit = len(n.Collectors)
		}
		log.Debugf("set ConcurrencyLimit: %d", concurrencyLimit)
	} else {
		concurrencyLimit = len(n.Collectors)
		log.Debugf("set default ConcurrencyLimit: %d", concurrencyLimit)
	}

	log.Debugf("launch collectors with ConcurrencyLimit: %d", concurrencyLimit)

	wgCollector := sync.WaitGroup{}
	wgSender := sync.WaitGroup{}

	// Create pipe channel used transmitting metrics from collectors to sender.
	pipelineIn := make(chan prometheus.Metric)

	// Run collectors.
	sem := make(chan struct{}, concurrencyLimit)
	wgCollector.Add(len(n.Collectors))
	for name, c := range n.Collectors {
		go func(name string, c Collector) {
			sem <- struct{}{}
			defer func() {
				<-sem
				wgCollector.Done()
			}()
			collect(name, n.Config, c, pipelineIn)
		}(name, c)
	}

	// Run sender.
	wgSender.Add(1)

	go func() {
		send(pipelineIn, out)
		wgSender.Done()
	}()

	// Wait until all collectors have been finished. Close the channel and allow to sender to send metrics.
	wgCollector.Wait()
	close(sem)
	close(pipelineIn)

	// Wait until metrics have been sent.
	wgSender.Wait()
}

// send acts like a middleware between metric collector functions which produces metrics and Prometheus who accepts metrics.
func send(in <-chan prometheus.Metric, out chan<- prometheus.Metric) {
	for m := range in {
		// Skip received nil values
		if m == nil {
			continue
		}

		// implement other middlewares here.

		out <- m
	}
}

// collect runs metric collection function and wraps it into instrumenting logic.
func collect(name string, config Config, c Collector, ch chan<- prometheus.Metric) {
	err := c.Update(config, ch)
	if err != nil {
		log.Errorf("%s collector failed; %s", name, err)
	}
}
