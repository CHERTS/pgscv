// Package collector is a pgSCV collectors
package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/cherts/pgscv/internal/http"
	"github.com/cherts/pgscv/internal/model"
	"github.com/prometheus/client_golang/prometheus"
)

type patroniCommonCollector struct {
	client               *http.Client
	up                   typedDesc
	name                 typedDesc
	version              typedDesc
	pgup                 typedDesc
	pgstart              typedDesc
	roleMaster           typedDesc
	roleStandbyLeader    typedDesc
	roleReplica          typedDesc
	xlogLoc              typedDesc
	xlogRecvLoc          typedDesc
	xlogReplLoc          typedDesc
	xlogReplTs           typedDesc
	xlogPaused           typedDesc
	pgversion            typedDesc
	unlocked             typedDesc
	timeline             typedDesc
	dcslastseen          typedDesc
	changetime           typedDesc
	replicationState     typedDesc
	pendingRestart       typedDesc
	pause                typedDesc
	inArchiveRecovery    typedDesc
	failsafeMode         typedDesc
	loopWait             typedDesc
	maximumLagOnFailover typedDesc
	retryTimeout         typedDesc
	ttl                  typedDesc
	syncStandby          typedDesc
}

// NewPatroniCommonCollector returns a new Collector exposing Patroni common info.
// For details see https://patroni.readthedocs.io/en/latest/rest_api.html#monitoring-endpoint
func NewPatroniCommonCollector(constLabels labels, settings model.CollectorSettings) (Collector, error) {
	varLabels := []string{"scope"}

	return &patroniCommonCollector{
		client: http.NewClient(http.ClientConfig{Timeout: time.Second}),
		up: newBuiltinTypedDesc(
			descOpts{"patroni", "", "up", "State of Patroni service: 1 is up, 0 otherwise.", 0},
			prometheus.GaugeValue,
			nil, constLabels,
			settings.Filters,
		),
		name: newBuiltinTypedDesc(
			descOpts{"patroni", "node", "name", "Node name.", 0},
			prometheus.GaugeValue,
			[]string{"scope", "node_name"}, constLabels,
			settings.Filters,
		),
		version: newBuiltinTypedDesc(
			descOpts{"patroni", "", "version", "Numeric representation of Patroni version.", 0},
			prometheus.GaugeValue,
			[]string{"scope", "version"}, constLabels,
			settings.Filters,
		),
		pgup: newBuiltinTypedDesc(
			descOpts{"patroni", "postgres", "running", "Value is 1 if Postgres is running, 0 otherwise.", 0},
			prometheus.GaugeValue,
			varLabels, constLabels,
			settings.Filters,
		),
		pgstart: newBuiltinTypedDesc(
			descOpts{"patroni", "postmaster", "start_time", "Epoch seconds since Postgres started.", 0},
			prometheus.GaugeValue,
			varLabels, constLabels,
			settings.Filters,
		),
		roleMaster: newBuiltinTypedDesc(
			descOpts{"patroni", "", "master", "Value is 1 if this node is the leader, 0 otherwise.", 0},
			prometheus.GaugeValue,
			varLabels, constLabels,
			settings.Filters,
		),
		roleStandbyLeader: newBuiltinTypedDesc(
			descOpts{"patroni", "", "standby_leader", "Value is 1 if this node is the standby_leader, 0 otherwise.", 0},
			prometheus.GaugeValue,
			varLabels, constLabels,
			settings.Filters,
		),
		roleReplica: newBuiltinTypedDesc(
			descOpts{"patroni", "", "replica", "Value is 1 if this node is a replica, 0 otherwise.", 0},
			prometheus.GaugeValue,
			varLabels, constLabels,
			settings.Filters,
		),
		xlogLoc: newBuiltinTypedDesc(
			descOpts{"patroni", "xlog", "location", "Current location of the Postgres transaction log, 0 if this node is a replica.", 0},
			prometheus.CounterValue,
			varLabels, constLabels,
			settings.Filters,
		),
		xlogRecvLoc: newBuiltinTypedDesc(
			descOpts{"patroni", "xlog", "received_location", "Current location of the received Postgres transaction log, 0 if this node is the leader.", 0},
			prometheus.CounterValue,
			varLabels, constLabels,
			settings.Filters,
		),
		xlogReplLoc: newBuiltinTypedDesc(
			descOpts{"patroni", "xlog", "replayed_location", "Current location of the replayed Postgres transaction log, 0 if this node is the leader.", 0},
			prometheus.CounterValue,
			varLabels, constLabels,
			settings.Filters,
		),
		xlogReplTs: newBuiltinTypedDesc(
			descOpts{"patroni", "xlog", "replayed_timestamp", "Current timestamp of the replayed Postgres transaction log, 0 if null.", 0},
			prometheus.GaugeValue,
			varLabels, constLabels,
			settings.Filters,
		),
		xlogPaused: newBuiltinTypedDesc(
			descOpts{"patroni", "xlog", "paused", "Value is 1 if the replaying of Postgres transaction log is paused, 0 otherwise.", 0},
			prometheus.GaugeValue,
			varLabels, constLabels,
			settings.Filters,
		),
		pgversion: newBuiltinTypedDesc(
			descOpts{"patroni", "postgres", "server_version", "Version of Postgres (if running), 0 otherwise.", 0},
			prometheus.GaugeValue,
			varLabels, constLabels,
			settings.Filters,
		),
		unlocked: newBuiltinTypedDesc(
			descOpts{"patroni", "cluster", "unlocked", "Value is 1 if the cluster is unlocked, 0 if locked.", 0},
			prometheus.GaugeValue,
			varLabels, constLabels,
			settings.Filters,
		),
		timeline: newBuiltinTypedDesc(
			descOpts{"patroni", "postgres", "timeline", "Postgres timeline of this node (if running), 0 otherwise.", 0},
			prometheus.CounterValue,
			varLabels, constLabels,
			settings.Filters,
		),
		replicationState: newBuiltinTypedDesc(
			descOpts{"patroni", "postgres", "streaming", "Value is 1 if Postgres is streaming, 0 otherwise.", 0},
			prometheus.CounterValue,
			varLabels, constLabels,
			settings.Filters,
		),
		dcslastseen: newBuiltinTypedDesc(
			descOpts{"patroni", "", "dcs_last_seen", "Epoch timestamp when DCS was last contacted successfully by Patroni.", 0},
			prometheus.CounterValue,
			varLabels, constLabels,
			settings.Filters,
		),
		changetime: newBuiltinTypedDesc(
			descOpts{"patroni", "last_timeline", "change_seconds", "Epoch seconds since latest timeline switched.", 0},
			prometheus.CounterValue,
			varLabels, constLabels,
			settings.Filters,
		),
		pendingRestart: newBuiltinTypedDesc(
			descOpts{"patroni", "", "pending_restart", "Value is 1 if the node needs a restart, 0 otherwise.", 0},
			prometheus.CounterValue,
			varLabels, constLabels,
			settings.Filters,
		),
		pause: newBuiltinTypedDesc(
			descOpts{"patroni", "", "is_paused", "Value is 1 if auto failover is disabled, 0 otherwise.", 0},
			prometheus.CounterValue,
			varLabels, constLabels,
			settings.Filters,
		),
		inArchiveRecovery: newBuiltinTypedDesc(
			descOpts{"patroni", "postgres", "in_archive_recovery", "Value is 1 if Postgres is replicating from archive, 0 otherwise.", 0},
			prometheus.CounterValue,
			varLabels, constLabels,
			settings.Filters,
		),
		failsafeMode: newBuiltinTypedDesc(
			descOpts{"patroni", "", "failsafe_mode_is_active", "Value is 1 if failsafe mode is active, 0 if inactive.", 0},
			prometheus.CounterValue,
			varLabels, constLabels,
			settings.Filters,
		),
		loopWait: newBuiltinTypedDesc(
			descOpts{"patroni", "", "loop_wait", "Current loop_wait setting of the Patroni configuration.", 0},
			prometheus.GaugeValue,
			varLabels, constLabels,
			settings.Filters,
		),
		maximumLagOnFailover: newBuiltinTypedDesc(
			descOpts{"patroni", "", "maximum_lag_on_failover", "Current maximum_lag_on_failover setting of the Patroni configuration.", 0},
			prometheus.GaugeValue,
			varLabels, constLabels,
			settings.Filters,
		),
		retryTimeout: newBuiltinTypedDesc(
			descOpts{"patroni", "", "retry_timeout", "Current retry_timeout setting of the Patroni configuration.", 0},
			prometheus.GaugeValue,
			varLabels, constLabels,
			settings.Filters,
		),
		ttl: newBuiltinTypedDesc(
			descOpts{"patroni", "", "ttl", "Current ttl setting of the Patroni configuration.", 0},
			prometheus.GaugeValue,
			varLabels, constLabels,
			settings.Filters,
		),
		syncStandby: newBuiltinTypedDesc(
			descOpts{"patroni", "", "sync_standby", "Value is 1 if synchronous mode is active, 0 if inactive.", 0},
			prometheus.GaugeValue,
			varLabels, constLabels,
			settings.Filters,
		),
	}, nil
}

// Update method collects statistics, parse it and produces metrics that are sent to Prometheus.
func (c *patroniCommonCollector) Update(_ context.Context, config Config, ch chan<- prometheus.Metric) error {
	if strings.HasPrefix(config.BaseURL, "https://") {
		c.client.EnableTLSInsecure()
	}

	// Check liveness.
	err := requestAPILiveness(c.client, config.BaseURL)
	if err != nil {
		ch <- c.up.newConstMetric(0)
		return err
	}

	ch <- c.up.newConstMetric(1)

	// Request general info.
	respInfo, err := requestAPIPatroni(c.client, config.BaseURL)
	if err != nil {
		return err
	}

	info, err := parsePatroniResponse(respInfo)
	if err != nil {
		return err
	}

	ch <- c.name.newConstMetric(0, info.scope, info.name)
	ch <- c.version.newConstMetric(info.version, info.scope, info.versionStr)
	ch <- c.pgup.newConstMetric(info.running, info.scope)
	ch <- c.pgstart.newConstMetric(info.startTime, info.scope)

	ch <- c.roleMaster.newConstMetric(info.master, info.scope)
	ch <- c.roleStandbyLeader.newConstMetric(info.standbyLeader, info.scope)
	ch <- c.roleReplica.newConstMetric(info.replica, info.scope)

	ch <- c.xlogLoc.newConstMetric(info.xlogLoc, info.scope)
	ch <- c.xlogRecvLoc.newConstMetric(info.xlogRecvLoc, info.scope)
	ch <- c.xlogReplLoc.newConstMetric(info.xlogReplLoc, info.scope)
	ch <- c.xlogReplTs.newConstMetric(info.xlogReplTs, info.scope)
	ch <- c.xlogPaused.newConstMetric(info.xlogPaused, info.scope)

	ch <- c.pgversion.newConstMetric(info.pgversion, info.scope)
	ch <- c.unlocked.newConstMetric(info.unlocked, info.scope)
	ch <- c.timeline.newConstMetric(info.timeline, info.scope)
	ch <- c.dcslastseen.newConstMetric(info.dcslastseen, info.scope)
	ch <- c.replicationState.newConstMetric(info.replicationState, info.scope)
	ch <- c.pendingRestart.newConstMetric(info.pendingRestart, info.scope)
	ch <- c.pause.newConstMetric(info.pause, info.scope)
	ch <- c.inArchiveRecovery.newConstMetric(info.inArchiveRecovery, info.scope)
	ch <- c.syncStandby.newConstMetric(info.syncStandby, info.scope)

	// Request and parse config.
	respConfig, err := requestAPIPatroniConfig(c.client, config.BaseURL)
	if err != nil {
		return err
	}

	patroniConfig, err := parsePatroniConfigResponse(respConfig)
	if err == nil {
		ch <- c.failsafeMode.newConstMetric(patroniConfig.failsafeMode, info.scope)
		ch <- c.loopWait.newConstMetric(patroniConfig.loopWait, info.scope)
		ch <- c.maximumLagOnFailover.newConstMetric(patroniConfig.maximumLagOnFailover, info.scope)
		ch <- c.retryTimeout.newConstMetric(patroniConfig.retryTimeout, info.scope)
		ch <- c.ttl.newConstMetric(patroniConfig.ttl, info.scope)
	}

	// Request and parse history.
	respHist, err := requestAPIHistory(c.client, config.BaseURL)
	if err != nil {
		return err
	}

	history, err := parseHistoryResponse(respHist)
	if err == nil {
		ch <- c.changetime.newConstMetric(history.lastTimelineChangeUnix, info.scope)
	}

	return nil
}

// requestAPILiveness requests to /liveness endpoint of API and returns error if failed.
func requestAPILiveness(c *http.Client, baseurl string) error {
	_, err := c.Get(baseurl + "/liveness")
	if err != nil {
		return err
	}

	return err
}

// patroniInfo implements 'patroni' object of API response.
type patroni struct {
	Version string `json:"version"`
	Scope   string `json:"scope"`
	Name    string `json:"name"`
}

// patroniXlogInfo implements 'xlog' object of API response.
type patroniXlogInfo struct {
	Location          int64  `json:"location"`           // master only
	ReceivedLocation  int64  `json:"received_location"`  // standby only
	ReplayedLocation  int64  `json:"replayed_location"`  // standby only
	ReplayedTimestamp string `json:"replayed_timestamp"` // standby only
	Paused            bool   `json:"paused"`             // standby only
}

// apiPatroniResponse implements API response returned by '/patroni' endpoint.
type apiPatroniResponse struct {
	State            string          `json:"state"`
	Unlocked         bool            `json:"cluster_unlocked"`
	Timeline         int             `json:"timeline"`
	PmStartTime      string          `json:"postmaster_start_time"`
	ServerVersion    int             `json:"server_version"`
	Patroni          patroni         `json:"patroni"`
	Role             string          `json:"role"`
	Xlog             patroniXlogInfo `json:"xlog"`
	DcsLastSeen      int             `json:"dcs_last_seen"`
	ReplicationState string          `json:"replication_state"`
	PendingRestart   bool            `json:"pending_restart"`
	Pause            bool            `json:"pause"`
	SyncStandby      bool            `json:"sync_standby"`
}

// patroniInfo implements metrics values extracted from the response of '/patroni' endpoint.
type patroniInfo struct {
	name              string
	scope             string
	version           float64
	versionStr        string
	running           float64
	startTime         float64
	master            float64
	standbyLeader     float64
	replica           float64
	xlogLoc           float64
	xlogRecvLoc       float64
	xlogReplLoc       float64
	xlogReplTs        float64
	xlogPaused        float64
	pgversion         float64
	unlocked          float64
	timeline          float64
	dcslastseen       float64
	replicationState  float64
	pendingRestart    float64
	pause             float64
	inArchiveRecovery float64
	syncStandby       float64
}

// apiPatroniConfigResponse implements API response returned by '/config' endpoint.
type apiPatroniConfigResponse struct {
	FailSafeMode     bool `json:"failsafe_mode"`
	LoopWait         int  `json:"loop_wait"`
	MaxLagOnFailover int  `json:"maximum_lag_on_failover"`
	RetryTimeout     int  `json:"retry_timeout"`
	TTL              int  `json:"ttl"`
}

// patroniConfigInfo implements metrics values extracted from the response of '/config' endpoint.
type patroniConfigInfo struct {
	failsafeMode         float64
	loopWait             float64
	maximumLagOnFailover float64
	retryTimeout         float64
	ttl                  float64
}

// requestAPIPatroniConfig requests to /config endpoint of API and returns parsed response.
func requestAPIPatroniConfig(c *http.Client, baseurl string) (*apiPatroniConfigResponse, error) {
	resp, err := c.Get(baseurl + "/config")
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("bad response: %s", resp.Status)
	}

	content, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	_ = resp.Body.Close()

	r := &apiPatroniConfigResponse{}

	err = json.Unmarshal(content, r)
	if err != nil {
		return nil, err
	}

	return r, nil
}

// parsePatroniConfigResponse parses info from API response and returns info object.
func parsePatroniConfigResponse(resp *apiPatroniConfigResponse) (*patroniConfigInfo, error) {
	var failsafeMode float64
	if resp.FailSafeMode {
		failsafeMode = 1
	}

	return &patroniConfigInfo{
		failsafeMode:         failsafeMode,
		loopWait:             float64(resp.LoopWait),
		maximumLagOnFailover: float64(resp.MaxLagOnFailover),
		retryTimeout:         float64(resp.RetryTimeout),
		ttl:                  float64(resp.TTL),
	}, nil
}

// requestAPIPatroni requests to /patroni endpoint of API and returns parsed response.
func requestAPIPatroni(c *http.Client, baseurl string) (*apiPatroniResponse, error) {
	resp, err := c.Get(baseurl + "/patroni")
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("bad response: %s", resp.Status)
	}

	content, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	_ = resp.Body.Close()

	r := &apiPatroniResponse{}

	err = json.Unmarshal(content, r)
	if err != nil {
		return nil, err
	}

	return r, nil
}

// parsePatroniResponse parses info from API response and returns info object.
func parsePatroniResponse(resp *apiPatroniResponse) (*patroniInfo, error) {
	version, err := semverStringToInt(resp.Patroni.Version)
	if err != nil {
		return nil, fmt.Errorf("parse version string '%s' failed: %s", resp.Patroni.Version, err)
	}

	var running float64
	if resp.State == "running" {
		running = 1
	}

	var PmStartTimeSec float64
	if resp.PmStartTime != "null" && resp.PmStartTime != "" {
		t1, err := time.Parse("2006-01-02 15:04:05.999999Z07:00", resp.PmStartTime)
		if err != nil {
			return nil, fmt.Errorf("parse patroni postmaster_start_time string '%s' failed: %s", resp.PmStartTime, err)
		}
		PmStartTimeSec = float64(t1.UnixNano()) / 1000000000
	}

	var master, stdleader, replica float64
	switch resp.Role {
	case "master":
		master, stdleader, replica = 1, 0, 0
	case "primary":
		master, stdleader, replica = 1, 0, 0
	case "standby_leader":
		master, stdleader, replica = 0, 1, 0
	case "replica":
		master, stdleader, replica = 0, 0, 1
	default:
		master, stdleader, replica = 1, 0, 0
	}

	var xlogReplTimeSecs float64
	if resp.Xlog.ReplayedTimestamp != "null" && resp.Xlog.ReplayedTimestamp != "" {
		t, err := time.Parse("2006-01-02 15:04:05.999999Z07:00", resp.Xlog.ReplayedTimestamp)
		if err != nil {
			return nil, fmt.Errorf("parse patroni xlog.replayed_timestamp string '%s' failed: %s", resp.PmStartTime, err)
		}
		xlogReplTimeSecs = float64(t.UnixNano()) / 1000000000
	}

	var xlogPaused float64
	if resp.Xlog.Paused {
		xlogPaused = 1
	}

	var unlocked float64
	if resp.Unlocked {
		unlocked = 1
	}

	var replicationState float64
	if resp.ReplicationState == "streaming" {
		replicationState = 1
	}

	var pendingRestart float64
	if resp.PendingRestart {
		pendingRestart = 1
	}

	var pause float64
	if resp.Pause {
		pause = 1
	}

	var inArchiveRecovery float64
	if resp.ReplicationState == "in archive recovery" {
		inArchiveRecovery = 1
	}

	var syncStandby float64
	if resp.SyncStandby {
		syncStandby = 1
	}

	return &patroniInfo{
		name:              resp.Patroni.Name,
		scope:             resp.Patroni.Scope,
		version:           float64(version),
		versionStr:        resp.Patroni.Version,
		running:           running,
		startTime:         PmStartTimeSec,
		master:            master,
		standbyLeader:     stdleader,
		replica:           replica,
		xlogLoc:           float64(resp.Xlog.Location),
		xlogRecvLoc:       float64(resp.Xlog.ReceivedLocation),
		xlogReplLoc:       float64(resp.Xlog.ReplayedLocation),
		xlogReplTs:        xlogReplTimeSecs,
		xlogPaused:        xlogPaused,
		pgversion:         float64(resp.ServerVersion),
		unlocked:          unlocked,
		timeline:          float64(resp.Timeline),
		dcslastseen:       float64(resp.DcsLastSeen),
		replicationState:  replicationState,
		pendingRestart:    pendingRestart,
		pause:             pause,
		inArchiveRecovery: inArchiveRecovery,
		syncStandby:       syncStandby,
	}, nil
}

// patroniHistoryUnit defines single item of Patroni history in the API response.
// Basically this is array like [ int, int, string, string ].
type patroniHistoryUnit []any

// apiHistoryResponse defines the API response with complete history.
type apiHistoryResponse []patroniHistoryUnit

// patroniHistory describes details (UNIX timestamp and reason) of the latest timeline change.
type patroniHistory struct {
	lastTimelineChangeReason string
	lastTimelineChangeUnix   float64
}

// requestAPIHistory requests /history endpoint of API and returns parsed response.
func requestAPIHistory(c *http.Client, baseurl string) (apiHistoryResponse, error) {
	resp, err := c.Get(baseurl + "/history")
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("bad response: %s", resp.Status)
	}

	content, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	_ = resp.Body.Close()

	r := apiHistoryResponse{}

	err = json.Unmarshal(content, &r)
	if err != nil {
		return nil, err
	}

	return r, nil
}

// parseHistoryResponse parses history and returns info about latest event in the history.
func parseHistoryResponse(resp apiHistoryResponse) (patroniHistory, error) {
	if len(resp) == 0 {
		return patroniHistory{}, nil
	}

	unit := resp[len(resp)-1]

	if len(unit) < 4 {
		return patroniHistory{}, fmt.Errorf("history unit invalid len")
	}

	// Check value types.
	reason, ok := unit[2].(string)
	if !ok {
		return patroniHistory{}, fmt.Errorf("history unit invalid message value type")
	}

	timestamp, ok := unit[3].(string)
	if !ok {
		return patroniHistory{}, fmt.Errorf("history unit invalid timestamp value type")
	}

	t, err := time.Parse("2006-01-02T15:04:05.999999Z07:00", timestamp)
	if err != nil {
		return patroniHistory{}, err
	}

	return patroniHistory{
		lastTimelineChangeReason: reason,
		lastTimelineChangeUnix:   float64(t.UnixNano()) / 1000000000,
	}, nil
}
