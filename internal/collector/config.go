// Package collector is a pgSCV collectors
package collector

import (
	"context"
	"fmt"
	"github.com/cherts/pgscv/internal/cache"
	"github.com/jackc/pgx/v5/pgxpool"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/cherts/pgscv/internal/log"
	"github.com/cherts/pgscv/internal/model"
	"github.com/cherts/pgscv/internal/store"
	"github.com/jackc/pgx/v5"
)

// Config defines collector's global configuration.
type Config struct {
	// ServiceType defines the type of discovered service. Depending on the type there should be different settings or
	// settings-specifics metric collection usecases.
	ServiceType string
	// ConnString defines a connection string used to connecting to the service
	ConnString string
	// BaseURL defines a URL string for connecting to HTTP service
	BaseURL string
	// NoTrackMode controls collector to gather and send sensitive information, such as queries texts.
	NoTrackMode bool
	// postgresServiceConfig defines collector's options specific for Postgres service
	postgresServiceConfig
	// Settings defines collectors settings propagated from main YAML configuration.
	Settings         model.CollectorsSettings
	CollectTopTable  int
	CollectTopIndex  int
	CollectTopQuery  int
	ConstLabels      *map[string]string
	TargetLabels     *map[string]string
	ConnTimeout      int // in seconds
	ConcurrencyLimit *int
	CacheConfig      *cache.Config
	DB               *store.DB
}

// postgresServiceConfig defines Postgres-specific stuff required during collecting Postgres metrics.
type postgresServiceConfig struct {
	// localService defines service is running on the local host.
	localService bool
	// blockSize defines size of data block Postgres operates.
	blockSize uint64
	// walSegmentSize defines size of WAL segment Postgres operates.
	walSegmentSize uint64
	// serverVersionNum defines version of Postgres in XXYYZZ format.
	serverVersionNum int
	// dataDirectory defines filesystem path where Postgres' data files and directories resides.
	dataDirectory string
	// loggingCollector defines value of 'logging_collector' and 'log_destination' GUC.
	loggingCollector bool
	logDestination   string
	// pgStatStatements defines is pg_stat_statements available in shared_preload_libraries and available for queries
	pgStatStatements bool
	// pgStatStatementsSchema defines the schema name where pg_stat_statements is installed
	pgStatStatementsSchema string
	rolConnLimit           int
}

func newPostgresServiceConfig(connStr string, connTimeout int) (postgresServiceConfig, error) {
	var config = postgresServiceConfig{}

	// Return empty config if empty connection string.
	if connStr == "" {
		return config, nil
	}

	pgconfig, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return config, err
	}
	if connTimeout > 0 {
		pgconfig.ConnConfig.ConnectTimeout = time.Duration(connTimeout) * time.Second
	}

	// Determine is service running locally.
	config.localService = isAddressLocal(pgconfig.ConnConfig.Host)

	conn, err := store.NewWithConfig(pgconfig)
	if err != nil {
		return config, err
	}
	defer conn.Close()

	var setting string

	// Get role connection limit.
	err = conn.Conn().QueryRow(context.Background(), "SELECT rolconnlimit FROM pg_roles WHERE rolname = USER").Scan(&setting)
	if err != nil {
		return config, fmt.Errorf("failed to get rolconnlimit setting from pg_roles, %s, please check user grants", err)
	}
	rolConnLimit, err := strconv.ParseInt(setting, 10, 32)
	if err != nil {
		return config, err
	}
	config.rolConnLimit = int(rolConnLimit)

	// Get Postgres block size.
	err = conn.Conn().QueryRow(context.Background(), "SELECT setting FROM pg_settings WHERE name = 'block_size'").Scan(&setting)
	if err != nil {
		return config, fmt.Errorf("failed to get block_size setting from pg_settings, %s, please check user grants", err)
	}
	bsize, err := strconv.ParseUint(setting, 10, 64)
	if err != nil {
		return config, err
	}

	config.blockSize = bsize

	// Get Postgres WAL segment size.
	err = conn.Conn().QueryRow(context.Background(), "SELECT setting FROM pg_settings WHERE name = 'wal_segment_size'").Scan(&setting)
	if err != nil {
		return config, fmt.Errorf("failed to get wal_segment_size setting from pg_settings, %s, please check user grants", err)
	}
	walSegSize, err := strconv.ParseUint(setting, 10, 64)
	if err != nil {
		return config, err
	}

	config.walSegmentSize = walSegSize

	// Get Postgres server version
	err = conn.Conn().QueryRow(context.Background(), "SELECT setting FROM pg_settings WHERE name = 'server_version_num'").Scan(&setting)
	if err != nil {
		return config, fmt.Errorf("failed to get server_version_num setting from pg_settings, %s, please check user grants", err)
	}
	version, err := strconv.Atoi(setting)
	if err != nil {
		return config, err
	}

	if version < PostgresVMinNum {
		log.Warnf("Postgres version is too old, some collectors functions won't work. Minimal required version is %s.", PostgresVMinStr)
	}

	config.serverVersionNum = version

	// Get Postgres data directory
	err = conn.Conn().QueryRow(context.Background(), "SELECT setting FROM pg_settings WHERE name = 'data_directory'").Scan(&setting)
	if err != nil {
		return config, fmt.Errorf("failed to get data_directory setting from pg_settings, %s, please check user grants", err)
	}

	config.dataDirectory = setting

	// Get setting of 'logging_collector' GUC.
	err = conn.Conn().QueryRow(context.Background(), "SELECT setting FROM pg_settings WHERE name = 'logging_collector'").Scan(&setting)
	if err != nil {
		return config, fmt.Errorf("failed to get logging_collector setting from pg_settings, %s, please check user grants", err)
	}

	if setting == "on" {
		config.loggingCollector = true
	}

	// Get setting of 'log_destination' GUC.
	err = conn.Conn().QueryRow(context.Background(), "SELECT setting FROM pg_settings WHERE name = 'log_destination'").Scan(&setting)
	if err != nil {
		return config, fmt.Errorf("failed to get log_destination setting from pg_settings, %s, please check user grants", err)
	}

	config.logDestination = setting

	// Discover pg_stat_statements.
	exists, schema, err := discoverPgStatStatements(conn)
	if err != nil {
		return config, err
	}

	if !exists {
		log.Warnln("pg_stat_statements not found, skip collecting statements metrics")
	}

	config.pgStatStatements = exists
	config.pgStatStatementsSchema = schema
	return config, nil
}

// FillPostgresServiceConfig defines new config for Postgres-based collectors.
func (cfg *Config) FillPostgresServiceConfig(connTimeout int) error {
	var err error
	cfg.postgresServiceConfig, err = newPostgresServiceConfig(cfg.ConnString, connTimeout)
	return err
}

// isAddressLocal return true if passed address is local, and return false otherwise.
func isAddressLocal(addr string) bool {
	if addr == "" {
		return false
	}

	if strings.HasPrefix(addr, "/") {
		return true
	}

	if addr == "localhost" || strings.HasPrefix(addr, "127.") || addr == "::1" {
		return true
	}

	addresses, err := net.InterfaceAddrs()
	if err != nil {
		// Consider error as the passed host address is not local
		log.Warnf("check network address '%s' failed: %s; consider it as remote", addr, err)
		return false
	}

	for _, a := range addresses {
		if strings.HasPrefix(a.String(), addr) {
			return true
		}
	}

	return false
}

// discoverPgStatStatements discovers pg_stat_statements, what schema it is installed.
func discoverPgStatStatements(conn *store.DB) (bool, string, error) {

	var setting string
	err := conn.Conn().QueryRow(context.Background(), "SELECT setting FROM pg_settings WHERE name = 'shared_preload_libraries'").Scan(&setting)
	if err != nil {
		conn.Close()
		return false, "", err
	}

	// If pg_stat_statements is not enabled globally, no reason to continue.
	if !strings.Contains(setting, "pg_stat_statements") {
		conn.Close()
		return false, "", nil
	}

	// Check for pg_stat_statements in default database specified in connection string.
	if schema := extensionInstalledSchema(conn, "pg_stat_statements"); schema != "" {
		conn.Close()
		return true, schema, nil
	}
	return false, "", nil
}

// extensionInstalledSchema returns schema name where extension is installed, or empty if not installed.
func extensionInstalledSchema(db *store.DB, name string) string {
	log.Debugf("check %s extension availability", name)

	var schema string
	err := db.Conn().
		QueryRow(context.Background(), "SELECT extnamespace::regnamespace FROM pg_extension WHERE extname = $1", name).
		Scan(&schema)
	if err != nil && err != pgx.ErrNoRows {
		log.Errorf("failed to check extensions '%s' in pg_extension: %s", name, err)
		return ""
	}

	return schema
}
