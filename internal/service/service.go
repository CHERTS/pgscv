// Package service is a pgSCV service helper
package service

import (
	"context"
	"fmt"
	"github.com/cherts/pgscv/internal/cache"
	"github.com/cherts/pgscv/internal/registry"
	"github.com/jackc/pgx/v5/pgxpool"
	"strings"
	"sync"
	"time"

	"slices"

	"github.com/cherts/pgscv/internal/collector"
	"github.com/cherts/pgscv/internal/http"
	"github.com/cherts/pgscv/internal/log"
	"github.com/cherts/pgscv/internal/model"
	"github.com/cherts/pgscv/internal/store"
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
	Collector    Collector
	ConstLabels  *map[string]string
	TargetLabels *map[string]string
	DB           *store.DB
}

const system0ServiceID = "system:0"

// Config defines service's configuration.
type Config struct {
	RuntimeMode        int
	NoTrackMode        bool
	ConnDefaults       map[string]string `yaml:"defaults"` // Defaults
	ConnsSettings      ConnsSettings
	DisabledCollectors []string
	// CollectorsSettings defines all collector settings propagated from main YAML configuration.
	CollectorsSettings model.CollectorsSettings
	CollectTopTable    int
	CollectTopIndex    int
	CollectTopQuery    int
	SkipConnErrorMode  bool
	ConstLabels        *map[string]*map[string]string
	TargetLabels       *map[string]*map[string]string
	ConnTimeout        int // in seconds
	ConcurrencyLimit   *int
	CacheConfig        *cache.Config
	CacheKey           string
	Pool               *pgxpool.Pool
	PoolerConfig       *PoolConfig
}

// PoolConfig defines pgxPool configuration.
type PoolConfig struct {
	MaxConns     *int32
	MinConns     *int32
	MinIdleConns *int32
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
	Registries   map[string]*registry.Registry
}

// NewRepository creates new services repository.
func NewRepository() *Repository {
	return &Repository{
		Services:   make(map[string]Service),
		Registries: make(map[string]*registry.Registry),
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

func (repo *Repository) addRegistry(serviceID string, r *registry.Registry) {
	repo.Lock()
	repo.Registries[serviceID] = r
	repo.Unlock()
}

// GetRegistry returns registry with specified serviceID
func (repo *Repository) GetRegistry(serviceID string) *registry.Registry {
	repo.RLock()
	r, ok := repo.Registries[serviceID]
	repo.RUnlock()
	if !ok {
		return nil
	}
	return r
}

// getService returns the service from repo with specified ID.
func (repo *Repository) getService(id string) Service {
	repo.RLock()
	s := repo.Services[id]
	repo.RUnlock()
	return s
}

// RemoveService remove service from repo and unregister prometheus collector
func (repo *Repository) RemoveService(id string) {
	repo.Lock()
	defer repo.Unlock()
	if s, ok := repo.Services[id]; ok {
		if s.Collector != nil {
			prometheus.Unregister(s.Collector)
		}
		delete(repo.Services, id)
	}
}

// totalServices returns the number of services registered in the repo.
func (repo *Repository) totalServices() int {
	repo.RLock()
	var size = len(repo.Services)
	repo.RUnlock()
	return size
}

// GetServiceIDs returns slice of services' IDs in the repo.
func (repo *Repository) GetServiceIDs() []string {
	var serviceIDs = make([]string, 0, repo.totalServices())
	repo.RLock()
	for i := range repo.Services {
		serviceIDs = append(serviceIDs, i)
	}
	repo.RUnlock()
	return serviceIDs
}

func (repo *Repository) serviceExists(serviceID string) bool {
	return slices.Contains(repo.GetServiceIDs(), serviceID)
}

// addServicesFromConfig reads info about services from the config file and fulfill the repo.
func (repo *Repository) addServicesFromConfig(config Config) {
	log.Debug("config: add services from configuration")

	if !repo.serviceExists(system0ServiceID) {
		// Always add system service.
		repo.addService(Service{ServiceID: system0ServiceID, ConnSettings: ConnSetting{ServiceType: model.ServiceTypeSystem}})
		log.Infof("registered new service [%s]", system0ServiceID)
	}

	// Sanity check, but basically should be always passed.
	if config.ConnsSettings == nil {
		log.Warn("connection settings for service are not defined, do nothing")
		return
	}

	wg := sync.WaitGroup{}
	wg.Add(len(config.ConnsSettings))
	// Check all passed connection settings and try to connect using them. Create a 'Service' instance
	// in the repo.
	for k, cs := range config.ConnsSettings {
		go func() {
			defer wg.Done()
			var msg string
			var db *store.DB
			if cs.ServiceType == model.ServiceTypePatroni {
				err := attemptRequest(cs.BaseURL)
				if err != nil {
					if config.SkipConnErrorMode {
						log.Warnf("%s: %s", cs.BaseURL, err)
					} else {
						log.Warnf("%s: %s, skip", cs.BaseURL, err)
						return
					}
				}
				msg = fmt.Sprintf("service [%s] available through: %s", k, cs.BaseURL)
			} else {
				// each ConnSetting struct is used for
				//   1) doing connection;
				//   2) getting connection properties to define service-specific parameters.
				pgconfig, err := pgxpool.ParseConfig(cs.Conninfo)
				if err != nil {
					log.Warnf("%s: %s, skip", cs.Conninfo, err)
					return
				}
				if config.ConnTimeout > 0 {
					pgconfig.ConnConfig.ConnectTimeout = time.Duration(config.ConnTimeout) * time.Second
				}

				if config.PoolerConfig != nil {
					if config.PoolerConfig != nil {
						pgconfig.MinConns = *config.PoolerConfig.MinConns
					}
					if config.PoolerConfig.MaxConns != nil {
						pgconfig.MaxConns = *config.PoolerConfig.MaxConns
					}
					if config.PoolerConfig.MinIdleConns != nil {
						pgconfig.MinIdleConns = *config.PoolerConfig.MinIdleConns
					}
				}

				// Check connection using created *ConnConfig, go next if connection failed.
				db, err = store.NewWithConfig(pgconfig)

				if err != nil {
					if config.SkipConnErrorMode {
						log.Warnf("%s@%s:%d/%s: %s", pgconfig.ConnConfig.User, pgconfig.ConnConfig.Host, pgconfig.ConnConfig.Port, pgconfig.ConnConfig.Database, err)
					} else {
						db.Close()
						log.Warnf("%s@%s:%d/%s: %s skip", pgconfig.ConnConfig.User, pgconfig.ConnConfig.Host, pgconfig.ConnConfig.Port, pgconfig.ConnConfig.Database, err)
						return
					}
				} else {
					if !config.SkipConnErrorMode {
						switch cs.ServiceType {
						case model.ServiceTypePostgresql:
							err = db.Conn().Ping(context.Background())
						case model.ServiceTypePgbouncer:
							var v string
							err = db.Conn().QueryRow(context.Background(), "SELECT 1").Scan(&v)
						}

						if err != nil {
							log.Warnf("%s@%s:%d/%s: %s skip", pgconfig.ConnConfig.User, pgconfig.ConnConfig.Host, pgconfig.ConnConfig.Port, pgconfig.ConnConfig.Database, err)
							db.Close()
							return
						}
					}

					msg = fmt.Sprintf("service [%s] available through: %s@%s:%d/%s", k, pgconfig.ConnConfig.User, pgconfig.ConnConfig.Host, pgconfig.ConnConfig.Port, pgconfig.ConnConfig.Database)
				}
			}

			// Create 'Service' struct with service-related properties and add it to service repo.
			s := Service{
				ServiceID:    k,
				ConnSettings: cs,
				Collector:    nil,
				DB:           db,
			}
			if config.ConstLabels != nil && (*config.ConstLabels)[k] != nil {
				s.ConstLabels = (*config.ConstLabels)[k]
			}
			if cs.TargetLabels == nil && config.TargetLabels != nil && (*config.TargetLabels)[k] != nil {
				s.TargetLabels = (*config.TargetLabels)[k]
			} else if cs.TargetLabels != nil {
				targetLabels := map[string]string{}
				for _, item := range *cs.TargetLabels {
					targetLabels[item.Name] = item.Value
				}
				s.TargetLabels = &targetLabels
			}

			// Use entry key as ServiceID unique identifier.
			repo.addService(s)
			log.Infof("registered new service [%s]", s.ServiceID)
			log.Debugln(msg)
		}()
	}
	wg.Wait()
}

// setupServices attaches metrics exporters to the services in the repo.
func (repo *Repository) setupServices(config Config) error {
	log.Debug("config: setting up services")
	sids := repo.GetServiceIDs()
	wg := sync.WaitGroup{}
	wg.Add(len(sids))
	var retErr error
	m := sync.Mutex{}

	for _, id := range sids {
		go func() {
			defer wg.Done()
			var service = repo.getService(id)
			if service.Collector == nil {
				factories := collector.Factories{}
				collectorConfig := collector.Config{
					NoTrackMode:      config.NoTrackMode,
					ServiceType:      service.ConnSettings.ServiceType,
					ConnString:       service.ConnSettings.Conninfo,
					Settings:         config.CollectorsSettings,
					CollectTopTable:  config.CollectTopTable,
					CollectTopIndex:  config.CollectTopIndex,
					CollectTopQuery:  config.CollectTopQuery,
					ConnTimeout:      config.ConnTimeout,
					ConcurrencyLimit: config.ConcurrencyLimit,
					CacheConfig:      config.CacheConfig,
					DB:               service.DB,
				}
				if config.ConstLabels != nil && (*config.ConstLabels)[id] != nil {
					collectorConfig.ConstLabels = (*config.ConstLabels)[id]
				}
				if config.TargetLabels != nil && (*config.TargetLabels)[id] != nil {
					collectorConfig.TargetLabels = (*config.TargetLabels)[id]
				}

				switch service.ConnSettings.ServiceType {
				case model.ServiceTypeSystem:
					factories.RegisterSystemCollectors(config.DisabledCollectors)
				case model.ServiceTypePostgresql:
					factories.RegisterPostgresCollectors(config.DisabledCollectors)
					err := collectorConfig.FillPostgresServiceConfig(config.ConnTimeout)
					if err != nil {
						log.Errorf("update service config failed: %s", err.Error())
					}
				case model.ServiceTypePgbouncer:
					factories.RegisterPgbouncerCollectors(config.DisabledCollectors)
				case model.ServiceTypePatroni:
					factories.RegisterPatroniCollectors(config.DisabledCollectors)
					collectorConfig.BaseURL = service.ConnSettings.BaseURL
				default:
					return
				}

				mc, err := collector.NewPgscvCollector(service.ServiceID, factories, collectorConfig)
				if err != nil {
					m.Lock()
					retErr = err
					m.Unlock()
				}
				service.Collector = mc

				// Register collector.
				prometheus.MustRegister(service.Collector)

				// Put updated service into repo.
				repo.addService(service)
				r := registry.NewRegistry(factories, collectorConfig)
				r.MustRegister(service.Collector)
				repo.addRegistry(service.ServiceID, r)
				log.Debugf("service configured [%s]", id)
			}
		}()
	}
	wg.Wait()
	return retErr
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
