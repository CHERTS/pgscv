// Package collector is a pgSCV collectors
package collector

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/cherts/pgscv/internal/log"
	"github.com/cherts/pgscv/internal/model"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	postgresTempFilesInflightQuery = "SELECT ts.spcname AS tablespace, COALESCE(COUNT(size), 0) AS files_total, COALESCE(sum(size), 0) AS bytes_total, " +
		"COALESCE(EXTRACT(EPOCH FROM clock_timestamp() - min(modification)), 0) AS max_age_seconds " +
		"FROM pg_tablespace ts LEFT JOIN (SELECT spcname,(pg_ls_tmpdir(oid)).* FROM pg_tablespace WHERE spcname != 'pg_global') ls ON ls.spcname = ts.spcname " +
		"WHERE ts.spcname != 'pg_global' GROUP BY ts.spcname"
)

type postgresStorageCollector struct {
	tempFiles       typedDesc
	tempBytes       typedDesc
	tempFilesMaxAge typedDesc
	datadirBytes    typedDesc
	tblspcBytes     typedDesc
	waldirBytes     typedDesc
	waldirFiles     typedDesc
	logdirBytes     typedDesc
	logdirFiles     typedDesc
	tmpfilesBytes   typedDesc
}

// NewPostgresStorageCollector returns a new Collector exposing various stats related to Postgres storage layer.
// This stats observed using different stats sources.
func NewPostgresStorageCollector(constLabels labels, settings model.CollectorSettings) (Collector, error) {
	return &postgresStorageCollector{
		tempFiles: newBuiltinTypedDesc(
			descOpts{"postgres", "temp_files", "in_flight", "Number of temporary files processed in flight.", 0},
			prometheus.GaugeValue,
			[]string{"tablespace"}, constLabels,
			settings.Filters,
		),
		tempBytes: newBuiltinTypedDesc(
			descOpts{"postgres", "temp_bytes", "in_flight", "Number of bytes occupied by temporary files processed in flight.", 0},
			prometheus.GaugeValue,
			[]string{"tablespace"}, constLabels,
			settings.Filters,
		),
		tempFilesMaxAge: newBuiltinTypedDesc(
			descOpts{"postgres", "temp_files", "max_age_seconds", "The age of the oldest temporary file, in seconds.", 0},
			prometheus.GaugeValue,
			[]string{"tablespace"}, constLabels,
			settings.Filters,
		),
		datadirBytes: newBuiltinTypedDesc(
			descOpts{"postgres", "data_directory", "bytes", "The size of Postgres server data directory, in bytes.", 0},
			prometheus.GaugeValue,
			[]string{"device", "mountpoint", "path"}, constLabels,
			settings.Filters,
		),
		tblspcBytes: newBuiltinTypedDesc(
			descOpts{"postgres", "tablespace_directory", "bytes", "The size of Postgres tablespace directory, in bytes.", 0},
			prometheus.GaugeValue,
			[]string{"tablespace", "device", "mountpoint", "path"}, constLabels,
			settings.Filters,
		),
		waldirBytes: newBuiltinTypedDesc(
			descOpts{"postgres", "wal_directory", "bytes", "The size of Postgres server WAL directory, in bytes.", 0},
			prometheus.GaugeValue,
			[]string{"device", "mountpoint", "path"}, constLabels,
			settings.Filters,
		),
		waldirFiles: newBuiltinTypedDesc(
			descOpts{"postgres", "wal_directory", "files", "The number of files in Postgres server WAL directory.", 0},
			prometheus.GaugeValue,
			[]string{"device", "mountpoint", "path"}, constLabels,
			settings.Filters,
		),
		logdirBytes: newBuiltinTypedDesc(
			descOpts{"postgres", "log_directory", "bytes", "The size of Postgres server LOG directory, in bytes.", 0},
			prometheus.GaugeValue,
			[]string{"device", "mountpoint", "path"}, constLabels,
			settings.Filters,
		),
		logdirFiles: newBuiltinTypedDesc(
			descOpts{"postgres", "log_directory", "files", "The number of files in Postgres server LOG directory.", 0},
			prometheus.GaugeValue,
			[]string{"device", "mountpoint", "path"}, constLabels,
			settings.Filters,
		),
		tmpfilesBytes: newBuiltinTypedDesc(
			descOpts{"postgres", "temp_files_all", "bytes", "The size of all Postgres temp directories, in bytes.", 0},
			prometheus.GaugeValue,
			[]string{"device", "mountpoint", "path"}, constLabels,
			settings.Filters,
		),
	}, nil
}

// Update method collects statistics, parse it and produces metrics.
func (c *postgresStorageCollector) Update(ctx context.Context, config Config, ch chan<- prometheus.Metric) error {
	// Following directory listing functions are available since:
	// - pg_ls_dir(), pg_ls_waldir() since Postgres 10
	// - pg_ls_tmpdir() since Postgres 12
	if config.pgVersion.Numeric < PostgresV10 {
		log.Debugln("[postgres storage collector]: some server-side functions are not available, required Postgres 10 or newer")
		return nil
	}

	conn := config.DB
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	var err error
	var cacheKey string
	var res *model.PGResult

	// Collecting in-flight temp only since Postgres 12.
	if config.pgVersion.Numeric >= PostgresV12 {
		cacheKey, res, _ = getFromCache(config.CacheConfig, config.ConnString, collectorPostgresStorage, postgresTempFilesInflightQuery)
		if res == nil {
			res, err = conn.Query(ctx, postgresTempFilesInflightQuery)
			if err != nil {
				log.Warnf("get in-flight temp files failed: %s; skip", err)
				return err
			}
			saveToCache(collectorPostgresStorage, wg, config.CacheConfig, cacheKey, res)
		}

		stats := parsePostgresTempFileInflght(res)
		for _, stat := range stats {
			ch <- c.tempFiles.newConstMetric(stat.tempfiles, stat.tablespace)
			ch <- c.tempBytes.newConstMetric(stat.tempbytes, stat.tablespace)
			ch <- c.tempFilesMaxAge.newConstMetric(stat.tempmaxage, stat.tablespace)
		}
	}

	// Collecting metrics about directories requires direct access to filesystems, which is impossible for remote services.
	// If the service is remote, collect a limited set of metrics about the Wal directory path, size and number of wal files.

	if !config.localService {
		// Collect a limited set of Wal metrics
		log.Debugln("[postgres storage collector]: collecting limited WAL, Log and Temp file metrics from remote services")
		dirstats, err := newPostgresStat(ctx, config, wg)
		if err != nil {
			return err
		}
		// WAL directory
		ch <- c.waldirBytes.newConstMetric(dirstats.waldirSizeBytes, "unknown", "unknown", dirstats.waldirPath)
		ch <- c.waldirFiles.newConstMetric(dirstats.waldirFilesCount, "unknown", "unknown", dirstats.waldirPath)

		// Log directory (only if logging_collector is enabled).
		if config.loggingCollector {
			ch <- c.logdirBytes.newConstMetric(dirstats.logdirSizeBytes, "unknown", "unknown", dirstats.logdirPath)
			ch <- c.logdirFiles.newConstMetric(dirstats.logdirFilesCount, "unknown", "unknown", dirstats.logdirPath)
		}

		// Temp directory
		if config.pgVersion.Numeric >= PostgresV12 {
			ch <- c.tmpfilesBytes.newConstMetric(dirstats.tmpfilesSizeBytes, "temp", "temp", "temp")
		}

		log.Debugln("[postgres storage collector]: skip collecting full directories metrics from remote services")
		return nil
	}

	// Collecting other server-directories stats (DATADIR and tablespaces, WALDIR, LOGDIR, TEMPDIR).
	dirstats, tblspcStats, err := newPostgresDirStat(ctx, config, wg)
	if err != nil {
		return err
	}

	// Data directory
	ch <- c.datadirBytes.newConstMetric(dirstats.datadirSizeBytes, dirstats.datadirDevice, dirstats.datadirMountpoint, dirstats.datadirPath)

	for _, ts := range tblspcStats {
		ch <- c.tblspcBytes.newConstMetric(ts.size, ts.name, ts.device, ts.mountpoint, ts.path)
	}

	// WAL directory
	ch <- c.waldirBytes.newConstMetric(dirstats.waldirSizeBytes, dirstats.waldirDevice, dirstats.waldirMountpoint, dirstats.waldirPath)
	ch <- c.waldirFiles.newConstMetric(dirstats.waldirFilesCount, dirstats.waldirDevice, dirstats.waldirMountpoint, dirstats.waldirPath)

	// Log directory (only if logging_collector is enabled).
	if config.loggingCollector {
		ch <- c.logdirBytes.newConstMetric(dirstats.logdirSizeBytes, dirstats.logdirDevice, dirstats.logdirMountpoint, dirstats.logdirPath)
		ch <- c.logdirFiles.newConstMetric(dirstats.logdirFilesCount, dirstats.logdirDevice, dirstats.logdirMountpoint, dirstats.logdirPath)
	}

	// Temp directory
	if config.pgVersion.Numeric >= PostgresV12 {
		ch <- c.tmpfilesBytes.newConstMetric(dirstats.tmpfilesSizeBytes, "temp", "temp", "temp")
	}

	return nil
}

// postgresTempfilesStat
type postgresTempfilesStat struct {
	tablespace string
	tempfiles  float64
	tempbytes  float64
	tempmaxage float64
}

// parsePostgresTempFileInflght parses PGResult, extract data and return struct with stats values.
func parsePostgresTempFileInflght(r *model.PGResult) map[string]postgresTempfilesStat {
	log.Debug("parse postgres storage stats")

	var stats = make(map[string]postgresTempfilesStat)

	// process row by row
	for _, row := range r.Rows {
		stat := postgresTempfilesStat{}

		// collect label values
		for i, colname := range r.Colnames {
			switch string(colname.Name) {
			case "tablespace":
				stat.tablespace = row[i].String
			}
		}

		// Define a map key as a tablespace name.
		tablespaceFQName := stat.tablespace

		// Put stats with labels (but with no data values yet) into stats store.
		stats[tablespaceFQName] = stat

		// fetch data values from columns
		for i, colname := range r.Colnames {
			// skip tablespace column - it's mapped as a label
			if string(colname.Name) == "tablespace" {
				continue
			}

			// Skip empty (NULL) values.
			if !row[i].Valid {
				continue
			}

			// Get data value and convert it to float64 used by Prometheus.
			v, err := strconv.ParseFloat(row[i].String, 64)
			if err != nil {
				log.Errorf("invalid input, parse '%s' failed: %s; skip", row[i].String, err)
				continue
			}

			// Run column-specific logic
			switch string(colname.Name) {
			case "files_total":
				s := stats[tablespaceFQName]
				s.tempfiles = v
				stats[tablespaceFQName] = s
			case "bytes_total":
				s := stats[tablespaceFQName]
				s.tempbytes = v
				stats[tablespaceFQName] = s
			case "max_age_seconds":
				s := stats[tablespaceFQName]
				s.tempmaxage = v
				stats[tablespaceFQName] = s
			default:
				continue
			}
		}
	}

	return stats
}

// postgresDirStat represents stats about Postgres system directories
type postgresDirStat struct {
	datadirPath       string
	datadirMountpoint string
	datadirDevice     string
	datadirSizeBytes  float64
	waldirPath        string
	waldirMountpoint  string
	waldirDevice      string
	waldirSizeBytes   float64
	waldirFilesCount  float64
	logdirPath        string
	logdirMountpoint  string
	logdirDevice      string
	logdirSizeBytes   float64
	logdirFilesCount  float64
	tmpfilesSizeBytes float64
	tmpfilesCount     float64
}

// newPostgresStat returns sizes of Postgres server directories.
func newPostgresStat(ctx context.Context, config Config, wg *sync.WaitGroup) (*postgresDirStat, error) {
	// Get Wal properties.
	waldirPath, waldirSize, waldirFilesCount, err := getWalStat(ctx, config, wg)
	if err != nil {
		log.Errorln(err)
	}

	logdirPath, logdirSize, logdirFilesCount, err := getLogStat(ctx, config, wg, config.loggingCollector)
	if err != nil {
		log.Errorln(err)
	}

	// Get temp files and directories properties.
	tmpfilesSize, tmpfilesCount, err := getTempfilesStat(ctx, config, config.pgVersion.Numeric, wg)
	if err != nil {
		log.Errorln(err)
	}

	// Return stats.
	return &postgresDirStat{
		waldirPath:        waldirPath,
		waldirSizeBytes:   float64(waldirSize),
		waldirFilesCount:  float64(waldirFilesCount),
		logdirPath:        logdirPath,
		logdirSizeBytes:   float64(logdirSize),
		logdirFilesCount:  float64(logdirFilesCount),
		tmpfilesSizeBytes: float64(tmpfilesSize),
		tmpfilesCount:     float64(tmpfilesCount),
	}, nil
}

// newPostgresDirStat returns sizes of Postgres server directories.
func newPostgresDirStat(ctx context.Context, config Config, wg *sync.WaitGroup) (*postgresDirStat, []tablespaceStat, error) {
	// Get directories mountpoints.
	mounts, err := getMountpoints()
	if err != nil {
		return nil, nil, fmt.Errorf("get mountpoints failed: %s", err)
	}

	// Get DATADIR properties.
	datadirDevice, datadirMount, datadirSize, err := getDatadirStat(config.dataDirectory, mounts)
	if err != nil {
		log.Errorln(err)
	}

	// Get tablespaces stats.
	tblspcStat, err := getTablespacesStat(ctx, config, wg, mounts)
	if err != nil {
		log.Errorln(err)
	}

	// Get WALDIR properties.
	waldirDevice, waldirPath, waldirMountpoint, waldirSize, waldirFilesCount, err := getWaldirStat(ctx, config, wg, mounts)
	if err != nil {
		log.Errorln(err)
	}

	// Get LOGDIR properties.
	logdirDevice, logdirPath, logdirMountpoint, logdirSize, logdirFilesCount, err := getLogdirStat(ctx, config, wg, config.loggingCollector, config.dataDirectory, mounts)
	if err != nil {
		log.Errorln(err)
	}

	// Get temp files and directories properties.
	tmpfilesSize, tmpfilesCount, err := getTempfilesStat(ctx, config, config.pgVersion.Numeric, wg)
	if err != nil {
		log.Errorln(err)
	}

	// Return stats and directories properties.
	return &postgresDirStat{
		datadirPath:       config.dataDirectory,
		datadirMountpoint: datadirMount,
		datadirDevice:     datadirDevice,
		datadirSizeBytes:  float64(datadirSize),
		waldirPath:        waldirPath,
		waldirMountpoint:  waldirMountpoint,
		waldirDevice:      waldirDevice,
		waldirSizeBytes:   float64(waldirSize),
		waldirFilesCount:  float64(waldirFilesCount),
		logdirPath:        logdirPath,
		logdirMountpoint:  logdirMountpoint,
		logdirDevice:      logdirDevice,
		logdirSizeBytes:   float64(logdirSize),
		logdirFilesCount:  float64(logdirFilesCount),
		tmpfilesSizeBytes: float64(tmpfilesSize),
		tmpfilesCount:     float64(tmpfilesCount),
	}, tblspcStat, nil
}

// getDatadirStat returns filesystem info related to DATADIR.
func getDatadirStat(datadir string, mounts []mount) (string, string, int64, error) {
	size, err := getDirectorySize(datadir)
	if err != nil {
		return "", "", 0, fmt.Errorf("get data_directory size failed: %s; skip", err)
	}

	// Find mountpoint and device for DATA directory.
	mountpoint, device, err := findMountpoint(mounts, datadir)
	if err != nil {
		return "", "", 0, fmt.Errorf("find data directory mountpoint failed: %s; skip", err)
	}

	device = truncateDeviceName(device)

	return device, mountpoint, size, nil
}

// tablespaceStat describes single Postgres tablespace.
type tablespaceStat struct {
	name       string
	device     string
	mountpoint string
	path       string
	size       float64
}

// getTablespacesStat returns filesystem info related to WALDIR.
func getTablespacesStat(ctx context.Context, config Config, wg *sync.WaitGroup, mounts []mount) ([]tablespaceStat, error) {
	var err error
	query := "select spcname, coalesce(nullif(pg_tablespace_location(oid), ''), current_setting('data_directory')) as path, pg_tablespace_size(oid) as size from pg_tablespace"
	cacheKey, res, _ := getFromCache(config.CacheConfig, config.ConnString, collectorPostgresStorage, query)
	if res == nil {
		res, err = config.DB.Query(ctx, query)
		if err != nil {
			return nil, err
		}
		saveToCache(collectorPostgresStorage, wg, config.CacheConfig, cacheKey, res)
	}

	var stats []tablespaceStat
	for _, row := range res.Rows {
		var name, path string
		var size float64
		if len(row) < 3 {
			log.Errorf("scan tablespaces row data failed")
			break
		}
		name = row[0].String
		path = row[1].String
		size, err = strconv.ParseFloat(row[2].String, 64)
		if err != nil {
			log.Errorf("scan tablespaces row data failed: %s", err)
			continue
		}

		mountpoint, device, err := findMountpoint(mounts, path)
		if err != nil {
			return nil, fmt.Errorf("find tablespaces mountpoint failed: %s", err)
		}

		device = truncateDeviceName(device)

		stats = append(stats, tablespaceStat{
			name:       name,
			device:     device,
			mountpoint: mountpoint,
			path:       path,
			size:       size,
		})
	}

	return stats, nil
}

// getWalStat returns Wal info related to WALDIR.
func getWalStat(ctx context.Context, config Config, wg *sync.WaitGroup) (string, int64, int64, error) {
	var err error
	var path string
	var size, count int64
	query := "SELECT current_setting('data_directory')||'/pg_wal' AS path, COALESCE(sum(size), 0) AS bytes, COALESCE(count(name), 0) AS count FROM pg_ls_waldir()"
	cacheKey, res, _ := getFromCache(config.CacheConfig, config.ConnString, collectorPostgresStorage, query)
	if res == nil {
		res, err = config.DB.Query(ctx, query)
		if err != nil {
			return "", 0, 0, err
		}
		saveToCache(collectorPostgresStorage, wg, config.CacheConfig, cacheKey, res)
	}
	if len(res.Rows) == 0 || len(res.Rows[0]) < 3 {
		return "", 0, 0, fmt.Errorf("error getWalStat")
	}
	row := res.Rows[0]
	path = row[0].String
	size, err = strconv.ParseInt(row[1].String, 10, 64)
	if err != nil {
		return "", 0, 0, err
	}
	count, err = strconv.ParseInt(row[2].String, 10, 64)
	if err != nil {
		return "", 0, 0, err
	}
	return path, size, count, nil
}

// getWaldirStat returns filesystem info related to WALDIR.
func getWaldirStat(ctx context.Context, config Config, wg *sync.WaitGroup, mounts []mount) (string, string, string, int64, int64, error) {
	path, size, count, err := getWalStat(ctx, config, wg)
	if err != nil {
		return "", "", "", 0, 0, err
	}

	mountpoint, device, err := findMountpoint(mounts, path)
	if err != nil {
		return "", "", "", 0, 0, fmt.Errorf("find WAL directory mountpoint failed: %s", err)
	}

	device = truncateDeviceName(device)

	return device, path, mountpoint, size, count, nil
}

// getLogStat returns Log info related to LOGDIR.
func getLogStat(ctx context.Context, config Config, wg *sync.WaitGroup, logcollector bool) (string, int64, int64, error) {
	if !logcollector {
		// Disabled logging_collector means all logs are written to stdout.
		// There is no reliable way to understand file location of stdout (it can be a symlink from /proc/pid/fd/1 -> somewhere)
		return "", 0, 0, nil
	}

	var size, count int64
	var path string
	var err error

	query := "SELECT current_setting('log_directory') AS path, COALESCE(sum(size), 0) AS bytes, COALESCE(count(name), 0) AS count FROM pg_ls_logdir()"

	cacheKey, res, _ := getFromCache(config.CacheConfig, config.ConnString, collectorPostgresStorage, query)
	if res == nil {
		res, err = config.DB.Query(ctx, query)
		if err != nil {
			return "", 0, 0, err
		}
		saveToCache(collectorPostgresStorage, wg, config.CacheConfig, cacheKey, res)
	}
	if len(res.Rows) == 0 || len(res.Rows[0]) < 3 {
		return "", 0, 0, fmt.Errorf("error getWalStat")
	}
	row := res.Rows[0]
	path = row[0].String
	size, err = strconv.ParseInt(row[1].String, 10, 64)
	if err != nil {
		return "", 0, 0, err
	}
	count, err = strconv.ParseInt(row[1].String, 10, 64)
	if err != nil {
		return "", 0, 0, err
	}
	return path, size, count, nil
}

// getLogdirStat returns filesystem info related to LOGDIR.
func getLogdirStat(ctx context.Context, config Config, wg *sync.WaitGroup, logcollector bool, datadir string, mounts []mount) (string, string, string, int64, int64, error) {
	path, size, count, err := getLogStat(ctx, config, wg, logcollector)
	if err != nil {
		return "", "", "", 0, 0, err
	}

	// Append path to DATADIR if it is not an absolute.
	if !strings.HasPrefix(path, "/") {
		path = datadir + "/" + path
	}

	// Find mountpoint and device for LOG directory.
	mountpoint, device, err := findMountpoint(mounts, path)
	if err != nil {
		return "", "", "", 0, 0, fmt.Errorf("find log directory mountpoint failed: %s", err)
	}

	device = truncateDeviceName(device)

	return device, path, mountpoint, size, count, nil
}

// getTempfilesStat returns filesystem info related to temp files and directories.
func getTempfilesStat(ctx context.Context, config Config, version int, wg *sync.WaitGroup) (int64, int64, error) {
	if version < PostgresV12 {
		return 0, 0, nil
	}
	var err error
	query := "SELECT coalesce(sum(size), 0) AS bytes, coalesce(count(name), 0) AS count FROM (SELECT (pg_ls_tmpdir(oid)).* FROM pg_tablespace WHERE spcname != 'pg_global') tablespaces"
	cacheKey, res, _ := getFromCache(config.CacheConfig, config.ConnString, collectorPostgresStorage, query)
	if res == nil {
		res, err = config.DB.Query(ctx, query)
		if err != nil {
			return 0, 0, err
		}
		saveToCache(collectorPostgresStorage, wg, config.CacheConfig, cacheKey, res)
	}
	if len(res.Rows) == 0 || len(res.Rows[0]) < 2 {
		return 0, 0, fmt.Errorf("error getWalStat")
	}
	row := res.Rows[0]
	var size, count int64
	size, err = strconv.ParseInt(row[0].String, 10, 64)
	if err != nil {
		return 0, 0, err
	}
	count, err = strconv.ParseInt(row[1].String, 10, 64)
	if err != nil {
		return 0, 0, err
	}
	return size, count, nil
}

// getDirectorySize walk through directory tree, calculate sizes and return total size of the directory.
func getDirectorySize(path string) (int64, error) {
	fi, err := os.Lstat(path)
	if err != nil {
		return 0, err
	}

	// If path is a symlink dereference it
	if fi.Mode()&os.ModeSymlink != 0 {
		resolved, err := os.Readlink(path)
		if err != nil {
			return 0, err
		}
		path = resolved
	}

	var size int64

	err = filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		// ignore ENOENT errors, they don't affect overall result.
		if err != nil {
			if strings.HasSuffix(err.Error(), "no such file or directory") {
				return nil
			}
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return err
	})
	return size, err
}

// findMountpoint checks path in the list of passed mountpoints.
func findMountpoint(mounts []mount, path string) (string, string, error) {
	fi, err := os.Lstat(path)
	if err != nil {
		return "", "", err
	}

	// If it is a symlink dereference it and try to find mountpoint again.
	if fi.Mode()&os.ModeSymlink != 0 {
		resolved, err := os.Readlink(path)
		if err != nil {
			return "", "", err
		}

		// if resolved path is not an absolute path, join it to dir where symlink has been found.
		if !strings.HasPrefix(resolved, "/") {
			dirs := strings.Split(path, "/")
			dirs[len(dirs)-1] = resolved
			resolved = strings.Join(dirs, "/")
		}

		return findMountpoint(mounts, resolved)
	}

	// Check path in a list of all mounts.
	for _, m := range mounts {
		if m.mountpoint == path {
			return path, m.device, nil
		}
	}

	// If path is not in mounts list, truncate path by one directory and try again.
	parts := strings.Split(path, "/")
	if len(parts) == 0 {
		return "", "", fmt.Errorf("mountpoint '%s' not found", path)
	}

	path = strings.Join(parts[0:len(parts)-1], "/")
	if path == "" {
		path = "/"
	}

	return findMountpoint(mounts, path)
}

// getMountpoints opens /proc/mounts file and run parser.
func getMountpoints() ([]mount, error) {
	file, err := os.Open("/proc/mounts")
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()

	return parseProcMounts(file)
}
