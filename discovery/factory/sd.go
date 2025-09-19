// Package factory create service discovery from config
package factory

import (
	"fmt"
	"github.com/cherts/pgscv/discovery"
	"github.com/cherts/pgscv/discovery/log"
	"github.com/cherts/pgscv/internal/discovery/service"
	"gopkg.in/yaml.v2"
)

// Instantiate returns initialized service discoverers (converting abstract configs to determined structures)
func Instantiate(discoveryConfig discovery.Config) (*map[string]discovery.Discovery, error) {
	log.Debug("[SD] Initializing discovery services...")
	var config = make(map[string]discovery.SdConfig)
	var out, err = yaml.Marshal(discoveryConfig)
	if err != nil {
		return nil, err
	}
	err = yaml.Unmarshal(out, config)
	if err != nil {
		return nil, err
	}
	var services = make(map[string]discovery.Discovery)
	for id, srv := range config {
		log.Debugf("[SD] Found service discovery type '%s'", srv.Type)
		switch srv.Type {
		case discovery.YandexMDB:
			services[id] = service.NewYandexDiscovery(id)
		case discovery.Postgres:
			services[id] = service.NewPostgresDiscovery(id)
		default:
			err := fmt.Errorf("[SD] Unknown service discovery type '%s'", srv.Type)
			log.Debug(err.Error())
			return nil, err
		}
		err := services[id].Init(srv.Config)
		if err != nil {
			log.Errorf("[SD] Failed to initializing discovery service '%s', error: %s", id, err.Error())
			return nil, err
		}
	}
	return &services, nil
}
