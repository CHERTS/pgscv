package service

import (
	"context"
	"fmt"
	"github.com/cherts/pgscv/discovery"
	"github.com/cherts/pgscv/discovery/log"
	"github.com/cherts/pgscv/internal/discovery/filter"
	"github.com/cherts/pgscv/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
	"gopkg.in/yaml.v2"
	"maps"
	"os"
	"sync"
	"time"
)

// PostgresDiscovery is main struct for Postgres discoverer
type PostgresDiscovery struct {
	sync.RWMutex
	id     string
	config postgresConfig

	subscribers map[string]subscriber
	db          *store.DB
	dbFilter    *filter.Filter
	dbConfig    *pgxpool.Config
}

type postgresConfig struct {
	ConnInfo        string  `json:"conninfo" yaml:"conninfo"`
	Db              *string `yaml:"db" json:"db"`
	ExcludeDb       *string `yaml:"exclude_db" json:"exclude_db"`
	password        string
	PasswordFromEnv *string  `json:"password_from_env" yaml:"password_from_env"`
	RefreshInterval int      `json:"refresh_interval" yaml:"refresh_interval"`
	TargetLabels    *[]Label `json:"target_labels" yaml:"target_labels"`
}

// NewPostgresDiscovery return pointer initialized PostgresDiscovery structure
func NewPostgresDiscovery(id string) *PostgresDiscovery {
	return &PostgresDiscovery{id: id, subscribers: make(map[string]subscriber)}
}

// Init implementation Init method of Discovery interface
func (p *PostgresDiscovery) Init(c discovery.Config) error {
	log.Debug(fmt.Sprintf("[Postgres:%s SD] Init discovery config...", p.id))
	pc, err := ensureConfigPostgres(c)
	if err != nil {
		log.Errorf("[Postgres SD] Failed to init discovery config, error: %v", err)
		return err
	}
	p.config = *pc
	p.dbFilter = filter.New(".*", p.config.Db, nil, p.config.ExcludeDb)
	return nil
}

// Start implementation Start method of Discovery interface
func (p *PostgresDiscovery) Start(ctx context.Context, errCh chan<- error) error {
	refreshInterval := time.Duration(p.config.RefreshInterval) * time.Second
	if refreshInterval == 0 {
		refreshInterval = 10 * time.Second
	}
	for {
		err := p.sync(ctx)
		if err != nil {
			log.Errorf("[Postgres:%s SD] Failed to sync, error: %s", p.id, err.Error())
			errCh <- err
		}
		select {
		case <-ctx.Done():
			log.Debug(fmt.Sprintf("[Postgres:%s SD] Context done.", p.id))
			return nil
		default:
			time.Sleep(refreshInterval)
		}
	}
}

// Subscribe implementation Subscribe method of Discovery interface
func (p *PostgresDiscovery) Subscribe(subscriberID string, addService discovery.AddServiceFunc, removeService discovery.RemoveServiceFunc) error {
	p.Lock()
	defer p.Unlock()
	p.subscribers[subscriberID] = subscriber{AddService: addService, RemoveService: removeService, syncedServices: make(map[string]discovery.Service), SyncedVersion: make(map[engineIdx]version)}
	return nil
}

// Unsubscribe implementation Unsubscribe method of Discovery interface
func (p *PostgresDiscovery) Unsubscribe(subscriberID string) error {
	p.Lock()
	defer p.Unlock()
	if _, ok := p.subscribers[subscriberID]; !ok {
		return nil
	}
	svc := make([]string, 0, len(p.subscribers[subscriberID].syncedServices))
	for k := range maps.Keys(p.subscribers[subscriberID].syncedServices) {
		svc = append(svc, k)
	}
	err := p.subscribers[subscriberID].RemoveService(svc)
	delete(p.subscribers, subscriberID)
	return err
}

func (p *PostgresDiscovery) sync(ctx context.Context) error {
	p.Lock()
	defer p.Unlock()
	err := p.ensureDB()
	if err != nil {
		return err
	}
	dbs, err := store.Databases(ctx, p.db)
	if err != nil {
		log.Errorf("[Postgres:%s SD] Failed to sync databases, error: %v", p.id, err)
		return nil
	}
	services := p.getServices(dbs)
	configLabels := &[]Label{
		{Name: "provider", Value: discovery.Postgres},
		{Name: "provider_id", Value: p.id},
	}
	err = syncSubscriberServices(discovery.Postgres, &p.subscribers, services, configLabels, p.config.TargetLabels)
	if err != nil {
		return err
	}
	return nil
}

func (p *PostgresDiscovery) getServices(dbs []string) *map[string]clusterDSN {
	services := make(map[string]clusterDSN, len(dbs))
	for _, db := range dbs {
		if !p.dbFilter.MatchDb(db) {
			continue
		}
		svcID := p.getSvcID(db)
		services[svcID] = clusterDSN{dsn: p.getDSN(db), name: svcID}
	}
	return &services
}

func (p *PostgresDiscovery) getSvcID(db string) string {
	return fmt.Sprintf("%s_%s", p.id, db)
}

func (p *PostgresDiscovery) getDSN(db string) string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
		p.dbConfig.ConnConfig.User,
		p.dbConfig.ConnConfig.Password,
		p.dbConfig.ConnConfig.Host,
		p.dbConfig.ConnConfig.Port,
		db)
}

func (p *PostgresDiscovery) ensureDB() error {
	if p.db != nil {
		return nil
	}
	var err error
	p.dbConfig, err = pgxpool.ParseConfig(p.config.ConnInfo)
	if err != nil {
		return err
	}
	if p.config.PasswordFromEnv != nil {
		p.dbConfig.ConnConfig.Password = p.config.password
	}
	s, err := store.NewWithConfig(p.dbConfig)
	if err != nil {
		return err
	}
	p.db = s
	return nil
}

func ensureConfigPostgres(config discovery.Config) (*postgresConfig, error) {
	c := &postgresConfig{}
	o, err := yaml.Marshal(config)
	if err != nil {
		return nil, err
	}
	err = yaml.Unmarshal(o, c)
	if err != nil {
		return nil, err
	}

	if c.PasswordFromEnv != nil && *c.PasswordFromEnv != "" {
		c.password = os.Getenv(*c.PasswordFromEnv)
	}
	return c, nil
}
