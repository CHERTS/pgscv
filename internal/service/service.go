// Package service is a pgSCV service helper
package service

import (
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/cherts/pgscv/internal/collector"
	"github.com/cherts/pgscv/internal/http"
	"github.com/cherts/pgscv/internal/log"
	"github.com/cherts/pgscv/internal/model"
	"github.com/cherts/pgscv/internal/store"
	"github.com/jackc/pgx/v4"
	"github.com/prometheus/client_golang/prometheus"
)

// Service struct describes service - the target from which should be collected metrics.
type Service struct {
	// Service identifier is unique key across all monitored services and used to distinguish services of the same type
	// running on the single host (two Postgres services running on the same host but listening on different ports).
	// Hence not to mix their metrics the ServiceID is introduced and attached to metrics as "sid" label:
	// metric_xact_commits{database="test", sid="postgres:5432"} -- metric from the first postgres running on 5432 port
	// metric_xact_commits{database="test", sid="postgres:5433"} -- metric from the second postgres running on 5433 port
	ServiceID string
	// Connection settings required for connecting to the service.
	ConnSettings ConnSetting
	// Prometheus-based metrics collector associated with the service. Each 'service' has its own dedicated collector instance
	// which implements a service-specific set of metric collectors.
	Collector Collector
}

// Config defines service's configuration.
type Config struct {
	RuntimeMode   int
	NoTrackMode   bool
	ConnDefaults  map[string]string `yaml:"defaults"` // Defaults
	ConnsSettings ConnsSettings
	// DatabasesRE defines regexp with databases from which builtin metrics should be collected.
	DatabasesRE        *regexp.Regexp
	DisabledCollectors []string
	// CollectorsSettings defines all collector settings propagated from main YAML configuration.
	CollectorsSettings model.CollectorsSettings
	CollectTopTable    int
	CollectTopIndex    int
	CollectTopQuery    int
	SkipConnErrorMode  bool
}

// Collector is an interface for prometheus.Collector.
type Collector interface {
	Describe(chan<- *prometheus.Desc)
	Collect(chan<- prometheus.Metric)
}

// Repository is the repository with services.
type Repository struct {
	sync.RWMutex                    // protect concurrent access
	Services     map[string]Service // service repo store
}

// NewRepository creates new services repository.
func NewRepository() *Repository {
	return &Repository{
		Services: make(map[string]Service),
	}
}

/* Public wrapper-methods of Repository */

// AddServicesFromConfig is a public wrapper on AddServicesFromConfig method.
func (repo *Repository) AddServicesFromConfig(config Config) {
	repo.addServicesFromConfig(config)
}

// SetupServices is a public wrapper on SetupServices method.
func (repo *Repository) SetupServices(config Config) error {
	return repo.setupServices(config)
}

/* Private methods of Repository */

// addService adds service to the repo.
func (repo *Repository) addService(s Service) {
	repo.Lock()
	repo.Services[s.ServiceID] = s
	repo.Unlock()
}

// getService returns the service from repo with specified ID.
func (repo *Repository) getService(id string) Service {
	repo.RLock()
	s := repo.Services[id]
	repo.RUnlock()
	return s
}

// totalServices returns the number of services registered in the repo.
func (repo *Repository) totalServices() int {
	repo.RLock()
	var size = len(repo.Services)
	repo.RUnlock()
	return size
}

// getServiceIDs returns slice of services' IDs in the repo.
func (repo *Repository) getServiceIDs() []string {
	var serviceIDs = make([]string, 0, repo.totalServices())
	repo.RLock()
	for i := range repo.Services {
		serviceIDs = append(serviceIDs, i)
	}
	repo.RUnlock()
	return serviceIDs
}

// addServicesFromConfig reads info about services from the config file and fulfill the repo.
func (repo *Repository) addServicesFromConfig(config Config) {
	log.Debug("config: add services from configuration")

	// Always add system service.
	repo.addService(Service{ServiceID: "system:0", ConnSettings: ConnSetting{ServiceType: model.ServiceTypeSystem}})
	log.Info("registered new service [system:0]")

	// Sanity check, but basically should be always passed.
	if config.ConnsSettings == nil {
		log.Warn("connection settings for service are not defined, do nothing")
		return
	}

	// Check all passed connection settings and try to connect using them. Create a 'Service' instance
	// in the repo.
	for k, cs := range config.ConnsSettings {
		var msg string

		if cs.ServiceType == model.ServiceTypePatroni {
			err := attemptRequest(cs.BaseURL)
			if err != nil {
				log.Warnf("%s: %s, skip", cs.BaseURL, err)
				continue
			}

			msg = fmt.Sprintf("service [%s] available through: %s", k, cs.BaseURL)
		} else {
			// each ConnSetting struct is used for
			//   1) doing connection;
			//   2) getting connection properties to define service-specific parameters.
			pgconfig, err := pgx.ParseConfig(cs.Conninfo)
			if err != nil {
				log.Warnf("%s: %s, skip", cs.Conninfo, err)
				continue
			}

			// Check connection using created *ConnConfig, go next if connection failed.
			db, err := store.NewWithConfig(pgconfig)
			if err != nil {
				if config.SkipConnErrorMode {
					log.Warnf("%s: %s", cs.Conninfo, err)
				} else {
					log.Warnf("%s: %s skip", cs.Conninfo, err)
					continue
				}
			} else {
				msg = fmt.Sprintf("service [%s] available through: %s@%s:%d/%s", k, pgconfig.User, pgconfig.Host, pgconfig.Port, pgconfig.Database)
				db.Close()
			}
		}

		// Create 'Service' struct with service-related properties and add it to service repo.
		s := Service{
			ServiceID:    k,
			ConnSettings: cs,
			Collector:    nil,
		}

		// Use entry key as ServiceID unique identifier.
		repo.addService(s)

		log.Infof("registered new service [%s]", s.ServiceID)

		log.Debugln(msg)
	}
}

// setupServices attaches metrics exporters to the services in the repo.
func (repo *Repository) setupServices(config Config) error {
	log.Debug("config: setting up services")

	for _, id := range repo.getServiceIDs() {
		var service = repo.getService(id)
		if service.Collector == nil {
			factories := collector.Factories{}
			collectorConfig := collector.Config{
				NoTrackMode:     config.NoTrackMode,
				ServiceType:     service.ConnSettings.ServiceType,
				ConnString:      service.ConnSettings.Conninfo,
				Settings:        config.CollectorsSettings,
				DatabasesRE:     config.DatabasesRE,
				CollectTopTable: config.CollectTopTable,
				CollectTopIndex: config.CollectTopIndex,
				CollectTopQuery: config.CollectTopQuery,
			}

			switch service.ConnSettings.ServiceType {
			case model.ServiceTypeSystem:
				factories.RegisterSystemCollectors(config.DisabledCollectors)
			case model.ServiceTypePostgresql:
				factories.RegisterPostgresCollectors(config.DisabledCollectors)
			case model.ServiceTypePgbouncer:
				factories.RegisterPgbouncerCollectors(config.DisabledCollectors)
			case model.ServiceTypePatroni:
				factories.RegisterPatroniCollectors(config.DisabledCollectors)
				collectorConfig.BaseURL = service.ConnSettings.BaseURL
			default:
				continue
			}

			mc, err := collector.NewPgscvCollector(service.ServiceID, factories, collectorConfig)
			if err != nil {
				return err
			}
			service.Collector = mc

			// Register collector.
			prometheus.MustRegister(service.Collector)

			// Put updated service into repo.
			repo.addService(service)
			log.Debugf("service configured [%s]", id)
		}
	}

	return nil
}

// attemptRequest tries to make a real HTTP request using passed URL string.
func attemptRequest(baseurl string) error {
	url := baseurl + "/health"
	log.Debugln("making test http request: ", url)

	var client = http.NewClient(http.ClientConfig{Timeout: time.Second})

	if strings.HasPrefix(url, "https://") {
		client.EnableTLSInsecure()
	}

	resp, err := client.Get(url) // #nosec G107
	if err != nil {
		return err
	}

	if (resp.StatusCode != http.StatusOK) && (resp.StatusCode != 503) {
		return fmt.Errorf("bad response: %s", resp.Status)
	}

	return nil
}
