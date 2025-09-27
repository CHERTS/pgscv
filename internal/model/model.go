// Package model is a pgSCV model
package model

import (
	"database/sql"
	"github.com/cherts/pgscv/internal/filter"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	// ServiceTypeSystem defines label string for system service.
	ServiceTypeSystem = "system"
	// ServiceTypePostgresql defines label string for Postgres services.
	ServiceTypePostgresql = "postgres"
	// ServiceTypePgbouncer defines label string for Pgbouncer services.
	ServiceTypePgbouncer = "pgbouncer"
	// ServiceTypePatroni defines label string for Patroni services.
	ServiceTypePatroni = "patroni"
)

// PGResult is the iterable store that contains query result (data and metadata) returned from Postgres
type PGResult struct {
	Nrows    int
	Ncols    int
	Colnames []pgconn.FieldDescription
	Rows     [][]sql.NullString
}

// Configurable collectors. It is possible to configure collectors using YAML
// configuration. Main goal - allow to define collecting of custom metrics.
// Use the following YAML structure:
//
//  collectors:                                                 <- Collectors (root level in YAML)
//    postgres/archiver:                                        <- CollectorSettings
//      filters:                                                <- CollectorSettings.Filters
//        query:                                                <- label
//          exclude: "(UPDATE|DELETE)"                          <- exclude metrics with labels contains these values
//      subsystems:                                             <- Subsystems
//        activity:                                             <- MetricsSubsystem
//          query: "SELECT l1, l2, l3, v1 FROM t1 WHERE t1"     <- MetricsSubsystem.Query
//          metrics:                                            <- MetricsSubsystem.Metrics
//            - name: l1                                        <- UserMetric
//              usage: COUNTER                                  <- UserMetric.Usage
//              value: v1                                       <- UserMetric.Value
//              labels: [ l1 ]                                  <- UserMetric.Labels
//              description: l1 description                     <- UserMetric.Description
//            - name: v1
//              usage: COUNTER
//              labeledValues:                                  <- UserMetric.LabeledValues
//                extra: [ l2, l3 ]
//              description: v1 description

// CollectorsSettings unions all collectors settings in one place.
type CollectorsSettings map[string]CollectorSettings

// CollectorSettings unions all settings related to a single collector.
type CollectorSettings struct {
	// Filters defines label-based filters applied to metrics.
	Filters filter.Filters `yaml:"filters"`
	// Subsystems defines subsystem with user-defined metrics.
	Subsystems Subsystems `yaml:"subsystems"`
}

// Subsystems unions all subsystems in one place.
type Subsystems map[string]MetricsSubsystem

// MetricsSubsystem describes a single subsystem.
type MetricsSubsystem struct {
	// Query defines a SQL statement used for getting label/values for metrics.
	Query string `yaml:"query"`
	// Metrics defines a list of labels and metrics should be extracted from Query result.
	Metrics Metrics `yaml:"metrics"`
}

// Metrics unions all metrics in one place.
type Metrics []UserMetric

// UserMetric defines a single user-defined metric and its properties.
type UserMetric struct {
	ShortName     string              `yaml:"name"`
	Usage         string              `yaml:"usage"`
	Labels        []string            `yaml:"labels,omitempty"`
	Value         string              `yaml:"value"`
	LabeledValues map[string][]string `yaml:"labeled_values,omitempty"`
	Description   string              `yaml:"description"`
}
