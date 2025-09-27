// Package main is a pgSCV main package
package main

import (
	"context"
	"fmt"
	"github.com/cherts/pgscv/discovery/factory"
	sdlog "github.com/cherts/pgscv/discovery/log"
	"github.com/cherts/pgscv/internal/cache"

	"os"
	"os/signal"
	"syscall"

	"github.com/alecthomas/kingpin/v2"
	"github.com/cherts/pgscv/internal/log"
	"github.com/cherts/pgscv/internal/pgscv"
	//_ "net/http/pprof"
)

var (
	appName, gitTag, gitCommit, gitBranch string
)

func main() {
	var (
		showVersion = kingpin.Flag("version", "show version and exit").Default().Bool()
		logLevel    = kingpin.Flag("log-level", "set log level: debug, info, warn, error").Default("info").Envar("LOG_LEVEL").String()
		configFile  = kingpin.Flag("config-file", "path to config file").Default("").Envar("PGSCV_CONFIG_FILE").String()
	)
	kingpin.Parse()
	log.SetLevel(*logLevel)
	log.SetApplication(appName)
	sdlog.Logger.Debug = log.Debug
	sdlog.Logger.Errorf = log.Errorf
	sdlog.Logger.Infof = log.Infof
	sdlog.Logger.Debugf = log.Debugf
	if *showVersion {
		fmt.Printf("%s %s %s-%s\n", appName, gitTag, gitCommit, gitBranch)
		os.Exit(0)
	}

	log.Infoln("starting ", appName, " ", gitTag, " ", gitCommit, "-", gitBranch)

	//go func() {
	//	log.Infoln(http.ListenAndServe(":6060", nil))
	//}()

	config, err := pgscv.NewConfig(*configFile)
	if err != nil {
		log.Errorln("create config failed: ", err)
		os.Exit(1)
	}

	if err := config.Validate(); err != nil {
		log.Errorln("validate config failed: ", err)
		os.Exit(1)
	}

	if config.DiscoveryConfig != nil {
		config.DiscoveryServices, err = factory.Instantiate(*config.DiscoveryConfig)
		if err != nil {
			log.Errorln("instantiate service discovery failed: ", err)
			os.Exit(1)
		}
	}

	if config.CacheConfig != nil {
		config.CacheConfig.Cache = cache.GetCacheClient(*config.CacheConfig, gitCommit)
	}

	ctx, cancel := context.WithCancel(context.Background())

	var doExit = make(chan error, 2)
	go func() {
		doExit <- listenSignals()
		cancel()
	}()

	go func() {
		doExit <- pgscv.Start(ctx, config)
		cancel()
	}()

	log.Warnf("received shutdown signal: '%s'", <-doExit)
}

func listenSignals() error {
	c := make(chan os.Signal, 1)
	defer signal.Stop(c)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	return fmt.Errorf("%s", <-c)
}
