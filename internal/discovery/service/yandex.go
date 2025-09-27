// Package service is package of service discovery module
package service

import (
	"context"
	"fmt"
	"github.com/cherts/pgscv/discovery"
	"maps"
	"os"
	"sync"
	"time"

	"github.com/cherts/pgscv/discovery/log"
	"github.com/cherts/pgscv/internal/discovery/cloud/yandex"
	"github.com/cherts/pgscv/internal/discovery/mapops"
	"gopkg.in/yaml.v2"
)

// Cluster config filters
type Cluster struct {
	Name string `yaml:"name" json:"name,required"`
	// when Db is nil, iterate over all db's
	Db          *string `yaml:"db" json:"db"`
	ExcludeName *string `yaml:"exclude_name" json:"exclude_name"`
	ExcludeDb   *string `yaml:"exclude_db" json:"exclude_db"`
}

// Label struct describe targets labels
type Label struct {
	Name  string `yaml:"name" json:"name"`
	Value string `yaml:"value" json:"value"`
}

// YandexConfig AuthorizedKey - path to json file, Password - password for
// databases, Clusters - array of structures with matching rules
type YandexConfig struct {
	AuthorizedKey   string    `json:"authorized_key" yaml:"authorized_key"`
	FolderID        string    `json:"folder_id" yaml:"folder_id"`
	User            string    `json:"user" yaml:"user"`
	Password        string    `json:"password" yaml:"password"`
	PasswordFromEnv string    `json:"password_from_env" yaml:"password_from_env"`
	RefreshInterval int       `json:"refresh_interval" yaml:"refresh_interval"`
	Clusters        []Cluster `json:"clusters" yaml:"clusters"`
	TargetLabels    *[]Label  `json:"target_labels" yaml:"target_labels"`
}

type engineIdx int
type version uint64

// YandexDiscovery is main struct for Yandex Managed Databases discoverer
type YandexDiscovery struct {
	sync.RWMutex
	id          string
	config      []YandexConfig
	engines     []*yandexEngine
	subscribers map[string]subscriber
}

// NewYandexDiscovery return pointer initialized YandexDiscovery structure
func NewYandexDiscovery(id string) *YandexDiscovery {
	return &YandexDiscovery{id: id, subscribers: make(map[string]subscriber)}
}

// Unsubscribe implementation Unsubscribe method of Discovery interface
func (yd *YandexDiscovery) Unsubscribe(subscriberID string) error {
	yd.Lock()
	defer yd.Unlock()
	if _, ok := yd.subscribers[subscriberID]; !ok {
		return nil
	}
	svc := make([]string, 0, len(yd.subscribers[subscriberID].syncedServices))
	for k := range maps.Keys(yd.subscribers[subscriberID].syncedServices) {
		svc = append(svc, k)
	}
	err := yd.subscribers[subscriberID].RemoveService(svc)
	delete(yd.subscribers, subscriberID)
	return err
}

// Subscribe implementation Subscribe method of Discovery interface
func (yd *YandexDiscovery) Subscribe(subscriberID string, addService discovery.AddServiceFunc, removeService discovery.RemoveServiceFunc) error {
	yd.Lock()
	defer yd.Unlock()
	log.Debugf("[Yandex.Cloud SD] Init subscribe '%s'", subscriberID)
	yd.subscribers[subscriberID] = subscriber{AddService: addService, RemoveService: removeService, syncedServices: make(map[string]discovery.Service), SyncedVersion: make(map[engineIdx]version)}
	for engineID, e := range yd.engines {
		e.RLock()
		for serviceID, svc := range e.dsn {
			labels := make(map[string]string)
			labels["mdb_cluster"] = svc.name
			labels["provider"] = discovery.YandexMDB
			labels["provider_id"] = yd.id
			targetLabels := make(map[string]string)
			if yd.config[engineID].TargetLabels != nil {
				for _, item := range *yd.config[engineID].TargetLabels {
					targetLabels[item.Name] = item.Value
				}
			}
			yd.subscribers[subscriberID].syncedServices[string(serviceID)] = discovery.Service{DSN: svc.dsn, ConstLabels: labels, TargetLabels: targetLabels}
		}
		e.RUnlock()
		yd.subscribers[subscriberID].SyncedVersion[engineIdx(engineID)] = e.version
	}
	if len(yd.subscribers[subscriberID].syncedServices) > 0 {
		err := addService(yd.subscribers[subscriberID].syncedServices)
		if err != nil {
			log.Errorf("[Yandex.Cloud SD] Error adding synced services: %v", err)
		}
		return err
	}
	return nil
}

func (yd *YandexDiscovery) sync() error {
	yd.Lock()
	defer yd.Unlock()
	log.Debug("[Yandex.Cloud SD] Sync...")
	for _, subscriber := range yd.subscribers {
		needSync := false
		engineServices := make(map[string]clusterDSN)
		for engineID, e := range yd.engines {
			e.RLock()
			if !(subscriber.SyncedVersion[engineIdx(engineID)] == e.version) {
				subscriber.SyncedVersion[engineIdx(engineID)] = e.version
				needSync = true
			}

			for serviceID, dsn := range e.dsn {
				engineServices[string(serviceID)] = dsn
			}
			e.RUnlock()
		}
		if !needSync {
			continue
		}

		removeSvc := make([]string, 0)
		appendSvc := make(map[string]discovery.Service)
		for _, v := range mapops.FullJoin(engineServices, subscriber.syncedServices) {
			if v.Left == nil {
				removeSvc = append(removeSvc, *v.Right)
				delete(subscriber.syncedServices, *v.Right)
			}
			if v.Right == nil {
				labels := make(map[string]string)
				labels["mdb_cluster"] = engineServices[*v.Left].name
				labels["provider"] = discovery.YandexMDB
				labels["provider_id"] = yd.id
				targetLabels := make(map[string]string)
				for _, l := range engineServices[*v.Left].labels {
					targetLabels[l.Name] = l.Value
				}
				appendSvc[*v.Left] = discovery.Service{DSN: engineServices[*v.Left].dsn, ConstLabels: labels, TargetLabels: targetLabels}
				subscriber.syncedServices[*v.Left] = appendSvc[*v.Left]
			}
		}
		if len(removeSvc) > 0 {
			log.Debugf("[Yandex.Cloud SD] Removing '%d' services from subscriber.", len(removeSvc))
			err := subscriber.RemoveService(removeSvc)
			if err != nil {
				return err
			}
		}
		if len(appendSvc) > 0 {
			log.Debugf("[Service Discovery] Appending '%d' services to subscriber.", len(appendSvc))
			err := subscriber.AddService(appendSvc)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Init implementation Init method of Discovery interface
func (yd *YandexDiscovery) Init(cfg discovery.Config) error {
	log.Debug(fmt.Sprintf("[Yandex.Cloud:%s SD] Init discovery config...", yd.id))
	c, err := ensureConfigYandexMDB(cfg)
	if err != nil {
		log.Errorf("[Yandex.Cloud SD] Failed to init discovery config, error: %v", err)
		return err
	}
	yd.config = c
	return nil
}

// Start implementation Start method of Discovery interface
func (yd *YandexDiscovery) Start(ctx context.Context, errCh chan<- error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for _, c := range yd.config {
		sdk, err := yandex.NewSDK(c.AuthorizedKey)
		if err != nil {
			log.Errorf("[Yandex.Cloud SD] Failed to creating Yandex SDK, error: %v", err)
			return err
		}
		var engine = yandexEngine{sdk: sdk, config: c, dsn: make(map[hostDb]clusterDSN)}
		err = engine.Start(ctx)
		if err != nil {
			log.Errorf("[Yandex.Cloud SD] Failed to starting Yandex engine, error: %v", err)
			return err
		}
		yd.engines = append(yd.engines, &engine)
	}

	for {
		err := yd.sync()
		if err != nil {
			log.Errorf("[Yandex.Cloud SD] Failed to sync, error: %s", err.Error())
			errCh <- err
		}
		select {
		case <-ctx.Done():
			log.Debug("[Yandex.Cloud SD] Context done.")
			return nil
		default:
			time.Sleep(10 * time.Second)
		}
	}
}

func ensureConfigYandexMDB(config discovery.Config) ([]YandexConfig, error) {
	c := &[]YandexConfig{}
	o, err := yaml.Marshal(config)
	if err != nil {
		return nil, err
	}
	err = yaml.Unmarshal(o, c)
	if err != nil {
		return nil, err
	}
	for i, yc := range *c {
		if yc.PasswordFromEnv != "" {
			(*c)[i].Password = os.Getenv(yc.PasswordFromEnv)
		}
	}
	return *c, nil
}
