// Package discovery is main package of service discovery module
package discovery

import (
	"context"
)

// Config abstract configuration SdConfig.config
type Config any

// Service abstract service definition
type Service struct {
	DSN          string
	ConstLabels  map[string]string
	TargetLabels map[string]string
}

// AddServiceFunc services arg is map serviceId -> data source name
type AddServiceFunc func(services map[string]Service) error

// RemoveServiceFunc serviceIds is array of service IDs
type RemoveServiceFunc func(serviceIds []string) error

// Discovery interface of abstract discovery services
type Discovery interface {
	//Init casting abstract Config to determined structure
	Init(c Config) error
	//Start is the discoverer starting point
	Start(ctx context.Context, errCh chan<- error) error
	//Subscribe - binding "add" and "remove" functions. Functions will be called when services appear or disappear
	Subscribe(subscriberID string, addService AddServiceFunc, removeService RemoveServiceFunc) error
	//Unsubscribe - remove subscriber from list
	Unsubscribe(subscriberID string) error
}

const (
	// YandexMDB constant SdConfig.type
	YandexMDB = "yandex-mdb"
	// Postgres constant SdConfig.type
	Postgres = "postgres"
)

// SdConfig top level of configuration tree
type SdConfig struct {
	Type   string `yaml:"type"`
	Config Config `yaml:"config"`
}
