// Package pgscv is a pgSCV main helper
package pgscv

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/cherts/pgscv/internal/http"
	"github.com/cherts/pgscv/internal/log"
	"github.com/cherts/pgscv/internal/model"
	"github.com/cherts/pgscv/internal/service"
	"github.com/jackc/pgx/v4"
	"gopkg.in/yaml.v2"
)

const (
	defaultListenAddress     = "127.0.0.1:9890"
	defaultPostgresUsername  = "pgscv"
	defaultPostgresDbname    = "postgres"
	defaultPgbouncerUsername = "pgscv"
	defaultPgbouncerDbname   = "pgbouncer"
)

// Config defines application's configuration.
type Config struct {
	NoTrackMode           bool                     `yaml:"no_track_mode"`      // controls tracking sensitive information (query texts, etc)
	ListenAddress         string                   `yaml:"listen_address"`     // Network address and port where the application should listen on
	ServicesConnsSettings service.ConnsSettings    `yaml:"services"`           // All connections settings for exact services
	Defaults              map[string]string        `yaml:"defaults"`           // Defaults
	DisableCollectors     []string                 `yaml:"disable_collectors"` // List of collectors which should be disabled. DEPRECATED in favor collectors settings
	CollectorsSettings    model.CollectorsSettings `yaml:"collectors"`         // Collectors settings propagated from main YAML configuration
	Databases             string                   `yaml:"databases"`          // Regular expression string specifies databases from which metrics should be collected
	DatabasesRE           *regexp.Regexp           // Regular expression object compiled from Databases
	AuthConfig            http.AuthConfig          `yaml:"authentication"`       // TLS and Basic auth configuration
	CollectTopTable       int                      `yaml:"collect_top_table"`    // Limit elements on Table collector
	CollectTopIndex       int                      `yaml:"collect_top_index"`    // Limit elements on Indexes collector
	CollectTopQuery       int                      `yaml:"collect_top_query"`    // Limit elements on Statements collector
	SkipConnErrorMode     bool                     `yaml:"skip_conn_error_mode"` // Skipping connection errors and creating a Service instance.
}

// NewConfig creates new config based on config file or return default config if config file is not specified.
func NewConfig(configFilePath string) (*Config, error) {
	// Get configuration from file
	var configFromFile *Config
	if configFilePath != "" {
		configRealPath, err := RealPath(configFilePath)
		if err != nil {
			return nil, err
		}
		log.Infoln("read configuration from ", configRealPath)
		content, err := os.ReadFile(filepath.Clean(configRealPath))
		if err != nil {
			return nil, err
		}
		configFromFile = &Config{Defaults: map[string]string{}}
		err = yaml.Unmarshal(content, configFromFile)
		if err != nil {
			return nil, err
		}
	}

	// Get configuration from environment variables
	configFromEnv, err := newConfigFromEnv()
	if err != nil {
		return nil, err
	}

	// Merge values from configFromFile and configFromEnv
	if configFromFile != nil {
		if configFromEnv.NoTrackMode {
			configFromFile.NoTrackMode = configFromEnv.NoTrackMode
		}
		if configFromEnv.ListenAddress != "" {
			configFromFile.ListenAddress = configFromEnv.ListenAddress
		}
		if len(configFromEnv.ServicesConnsSettings) > 0 {
			configFromFile.ServicesConnsSettings = mergeServicesConnsSettings(configFromFile.ServicesConnsSettings, configFromEnv.ServicesConnsSettings)
		}
		for key, value := range configFromEnv.Defaults {
			configFromFile.Defaults[key] = value
		}
		configFromFile.DisableCollectors = append(configFromFile.DisableCollectors, configFromEnv.DisableCollectors...)
		configFromFile.CollectorsSettings = mergeCollectorsSettings(configFromFile.CollectorsSettings, configFromEnv.CollectorsSettings)

		if configFromEnv.Databases != "" {
			// If set environment variable PGSCV_DATABASES and 'databases' settings from file is empty, then use PGSCV_DATABASES
			if configFromFile.Databases == "" {
				configFromFile.Databases = configFromEnv.Databases
			} else {
				// If set environment variable PGSCV_DATABASES and 'databases' settings from file is not empty, then use 'databases' settings from file
				log.Debug("PGSCV_DATABASES environment setting was ignored, the settings from configuration file were used.")
			}
		}
		// Set AuthConfig settings
		if configFromEnv.AuthConfig != (http.AuthConfig{}) {
			configFromFile.AuthConfig = configFromEnv.AuthConfig
		}

		if configFromEnv.CollectTopTable > 0 {
			configFromFile.CollectTopTable = configFromEnv.CollectTopTable
		}
		if configFromEnv.CollectTopIndex > 0 {
			configFromFile.CollectTopIndex = configFromEnv.CollectTopIndex
		}
		if configFromEnv.CollectTopQuery > 0 {
			configFromFile.CollectTopQuery = configFromEnv.CollectTopQuery
		}
		if configFromEnv.SkipConnErrorMode {
			configFromFile.SkipConnErrorMode = configFromEnv.SkipConnErrorMode
		}
		return configFromFile, nil
	}

	return configFromEnv, nil
}

// Merge CollectorsSettings
func mergeCollectorsSettings(dest, src model.CollectorsSettings) model.CollectorsSettings {
	if dest == nil {
		return src
	}
	for key, value := range src {
		dest[key] = value
	}
	return dest
}

// Merge services ConnsSettings
func mergeServicesConnsSettings(dest, src service.ConnsSettings) service.ConnsSettings {
	if dest == nil {
		return src
	}
	for key, value := range src {
		dest[key] = value
	}

	return dest
}

// RealPath read real config file path
func RealPath(filePath string) (string, error) {
	log.Infoln("reading file information ", filePath)
	fileInfo, err := os.Lstat(filepath.Clean(filePath))
	if err != nil {
		return filePath, err
	}
	if fileInfo.Mode()&fs.ModeSymlink != 0 {
		log.Debugln("is symlink")
		link, err := filepath.EvalSymlinks(filePath)
		if err != nil {
			return filePath, err
		}
		log.Debugln("resolved symlink to ", link)
		return link, nil
	} else if fileInfo.Mode().IsRegular() {
		log.Debugln("is regular file")
		return filePath, nil
	} else if fileInfo.Mode()&fs.ModeNamedPipe != 0 {
		log.Debugln("is named pipe")
		return filePath, nil
	} else if fileInfo.Mode().IsDir() {
		log.Debugln("is directory")
		return filePath, err
	} else {
		return filePath, err
	}
}

// Validate checks configuration for stupid values and set defaults
func (c *Config) Validate() error {
	if c.ListenAddress == "" {
		c.ListenAddress = defaultListenAddress
	}

	if c.NoTrackMode {
		log.Infoln("no-track enabled for [pg_stat_statements.query].")
	} else {
		log.Infoln("no-track disabled, for details check the documentation about 'no_track_mode' option.")
	}

	if c.SkipConnErrorMode {
		log.Infoln("skipping connection errors is enabled.")
	}

	// setup defaults
	if c.Defaults == nil {
		c.Defaults = map[string]string{}
	}

	if _, ok := c.Defaults["postgres_username"]; !ok {
		c.Defaults["postgres_username"] = defaultPostgresUsername
	}

	if _, ok := c.Defaults["postgres_dbname"]; !ok {
		c.Defaults["postgres_dbname"] = defaultPostgresDbname
	}

	if _, ok := c.Defaults["pgbouncer_username"]; !ok {
		c.Defaults["pgbouncer_username"] = defaultPgbouncerUsername
	}

	if _, ok := c.Defaults["pgbouncer_dbname"]; !ok {
		c.Defaults["pgbouncer_dbname"] = defaultPgbouncerDbname
	}

	// User might specify its own set of services which he would like to monitor. This services should be validated and
	// invalid should be rejected. Validation is performed using pgx.ParseConfig method which does all dirty work.
	if c.ServicesConnsSettings != nil {
		if len(c.ServicesConnsSettings) != 0 {
			for k, s := range c.ServicesConnsSettings {
				if k == "" {
					return fmt.Errorf("empty service specified")
				}
				if s.ServiceType == "" {
					return fmt.Errorf("empty service_type for %s", k)
				}

				_, err := pgx.ParseConfig(s.Conninfo)
				if err != nil {
					return fmt.Errorf("invalid conninfo for %s: %s", k, err)
				}
			}
		}
	}

	// Create 'databases' regexp object for builtin metrics.
	re, err := newDatabasesRegexp(c.Databases)
	if err != nil {
		return err
	}
	c.DatabasesRE = re

	// Validate collector settings.
	err = validateCollectorSettings(c.CollectorsSettings)
	if err != nil {
		return err
	}

	// Validate authentication settings.
	enableAuth, enableTLS, err := c.AuthConfig.Validate()
	if err != nil {
		return err
	}
	c.AuthConfig.EnableAuth = enableAuth
	c.AuthConfig.EnableTLS = enableTLS

	if c.CollectTopQuery < 0 || c.CollectTopQuery > 1000 {
		return fmt.Errorf("invalid setting 'collect_top_query' or env PGSCV_COLLECT_TOP_QUERY (value '%d'), allowed 0 to 1000", c.CollectTopQuery)
	}
	if c.CollectTopTable < 0 || c.CollectTopTable > 1000 {
		return fmt.Errorf("invalid setting 'collect_top_table' or env PGSCV_COLLECT_TOP_TABLE (value '%d'), allowed 0 to 1000", c.CollectTopTable)
	}
	if c.CollectTopIndex < 0 || c.CollectTopIndex > 1000 {
		return fmt.Errorf("invalid setting 'collect_top_index' or env PGSCV_COLLECT_TOP_INDEX (value '%d'), allowed 0 to 1000", c.CollectTopIndex)
	}

	if c.CollectTopQuery > 0 {
		log.Infof("TopQuery: limit (%d queries) enabled", c.CollectTopQuery)
	}
	if c.CollectTopTable > 0 {
		log.Infof("TopTable: limit (%d tables) enabled", c.CollectTopTable)
	}
	if c.CollectTopIndex > 0 {
		log.Infof("TopIndex: limit (%d indexes) enabled", c.CollectTopIndex)
	}

	return nil
}

// validateCollectorSettings validates collectors settings passed from main YAML configuration.
func validateCollectorSettings(cs model.CollectorsSettings) error {
	if len(cs) == 0 {
		return nil
	}

	for csName, settings := range cs {
		re1 := regexp.MustCompile(`^[a-zA-Z0-9]+/[a-zA-Z0-9]+$`)
		if !re1.MatchString(csName) {
			return fmt.Errorf("invalid collector name: %s", csName)
		}

		err := settings.Filters.Compile()
		if err != nil {
			return err
		}

		// Validate subsystems level
		for ssName, subsys := range settings.Subsystems {
			re2 := regexp.MustCompilePOSIX(`^[a-zA-Z0-9_]+$`)

			if !re2.MatchString(ssName) {
				return fmt.Errorf("invalid subsystem name: %s", ssName)
			}

			// Validate databases regexp.
			_, err := regexp.Compile(subsys.Databases)
			if err != nil {
				return fmt.Errorf("databases invalid regular expression specified: %s", err)
			}

			// Query must be specified if any metrics.
			if len(subsys.Metrics) > 0 && subsys.Query == "" {
				return fmt.Errorf("query is not specified for subsystem '%s' metrics", ssName)
			}

			// Validate metrics level
			reMetric := regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

			for _, m := range subsys.Metrics {
				if m.Value == "" && m.LabeledValues == nil {
					return fmt.Errorf("value or labeled_values should be specified for metric '%s'", m.ShortName)
				}

				if m.Value != "" && m.LabeledValues != nil {
					return fmt.Errorf("value and labeled_values cannot be used together for metric '%s'", m.ShortName)
				}

				usage := m.Usage
				switch usage {
				case "COUNTER", "GAUGE":
					if !reMetric.MatchString(m.ShortName) {
						return fmt.Errorf("invalid metric name '%s'", m.ShortName)
					}
					if m.Description == "" {
						return fmt.Errorf("metric description is not specified for %s", m.ShortName)
					}
				default:
					return fmt.Errorf("invalid metric usage '%s'", usage)
				}
			}
		}
	}

	return nil
}

// newConfigFromEnv create config using environment variables.
func newConfigFromEnv() (*Config, error) {
	log.Infoln("read configuration from environment")

	config := &Config{
		Defaults:              map[string]string{},
		ServicesConnsSettings: map[string]service.ConnSetting{},
	}

	for _, env := range os.Environ() {
		if !strings.HasPrefix(env, "PGSCV_") &&
			!strings.HasPrefix(env, "POSTGRES_DSN") &&
			!strings.HasPrefix(env, "DATABASE_DSN") &&
			!strings.HasPrefix(env, "PGBOUNCER_DSN") &&
			!strings.HasPrefix(env, "PATRONI_URL") {
			continue
		}

		ff := strings.SplitN(env, "=", 2)

		key, value := ff[0], ff[1]

		// Parse POSTGRES_DSN (or its alias DATABASE_DSN).
		if strings.HasPrefix(key, "POSTGRES_DSN") || strings.HasPrefix(key, "DATABASE_DSN") {
			id, cs, err := service.ParsePostgresDSNEnv(key, value)
			if err != nil {
				return nil, err
			}

			config.ServicesConnsSettings[id] = cs
		}

		// Parse PGBOUNCER_DSN.
		if strings.HasPrefix(key, "PGBOUNCER_DSN") {
			id, cs, err := service.ParsePgbouncerDSNEnv(key, value)
			if err != nil {
				return nil, err
			}

			config.ServicesConnsSettings[id] = cs
		}

		// Parse PATRONI_URL.
		if strings.HasPrefix(key, "PATRONI_URL") {
			id, cs, err := service.ParsePatroniURLEnv(key, value)
			if err != nil {
				return nil, err
			}

			config.ServicesConnsSettings[id] = cs
		}

		switch key {
		case "PGSCV_LISTEN_ADDRESS":
			config.ListenAddress = value
		case "PGSCV_NO_TRACK_MODE":
			config.NoTrackMode = toBool(value)
		case "PGSCV_DATABASES":
			config.Databases = value
		case "PGSCV_DISABLE_COLLECTORS":
			config.DisableCollectors = strings.Split(strings.Replace(value, " ", "", -1), ",")
		case "PGSCV_AUTH_USERNAME":
			config.AuthConfig.Username = value
		case "PGSCV_AUTH_PASSWORD":
			config.AuthConfig.Password = value
		case "PGSCV_AUTH_KEYFILE":
			config.AuthConfig.Keyfile = value
		case "PGSCV_AUTH_CERTFILE":
			config.AuthConfig.Certfile = value
		case "PGSCV_COLLECT_TOP_QUERY":
			collectTopQuery, err := strconv.Atoi(value)
			if err != nil {
				return nil, fmt.Errorf("invalid setting PGSCV_COLLECT_TOP_QUERY, value '%s', allowed only digits", value)
			}
			config.CollectTopQuery = collectTopQuery
		case "PGSCV_COLLECT_TOP_TABLE":
			collectTopTable, err := strconv.Atoi(value)
			if err != nil {
				return nil, fmt.Errorf("invalid setting PGSCV_COLLECT_TOP_TABLE, value '%s', allowed only digits", value)
			}
			config.CollectTopTable = collectTopTable
		case "PGSCV_COLLECT_TOP_INDEX":
			collectTopIndex, err := strconv.Atoi(value)
			if err != nil {
				return nil, fmt.Errorf("invalid setting PGSCV_COLLECT_TOP_INDEX, value '%s', allowed only digits", value)
			}
			config.CollectTopIndex = collectTopIndex
		case "PGSCV_SKIP_CONN_ERROR_MODE":
			config.SkipConnErrorMode = toBool(value)
		}
	}
	return config, nil
}

// toBool string to bool
func toBool(s string) bool {
	switch s {
	case "y", "yes", "Yes", "YES", "t", "true", "True", "TRUE", "1", "on":
		return true
	case "n", "no", "No", "NO", "f", "false", "False", "FALSE", "0", "off":
		return false
	default:
		return false
	}
}

// newDatabasesRegexp creates new regexp depending on passed string.
func newDatabasesRegexp(s string) (*regexp.Regexp, error) {
	if s == "" {
		s = ".+"
	}

	return regexp.Compile(s)
}
