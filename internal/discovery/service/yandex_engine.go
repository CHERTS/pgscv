package service

import (
	"context"
	"fmt"
	"github.com/cherts/pgscv/internal/discovery/filter"
	"maps"
	"strings"
	"sync"
	"time"

	"github.com/cherts/pgscv/discovery/log"
	"github.com/cherts/pgscv/internal/discovery/cloud/yandex"
)

type hostDb string

type yandexEngine struct {
	sync.RWMutex
	sdk     *yandex.SDK
	config  YandexConfig
	dsn     map[hostDb]clusterDSN
	version version
}

func (ye *yandexEngine) Start(ctx context.Context) error {
	go func() {
		ye.RLock()
		interval := time.Duration(ye.config.RefreshInterval) * time.Minute
		folderID := ye.config.FolderID
		f := make([]filter.Filter, 0, len(ye.config.Clusters))
		password := ye.config.Password
		username := ye.config.User
		for _, c := range ye.config.Clusters {
			f = append(f, *filter.New(c.Name, c.Db, c.ExcludeName, c.ExcludeDb))
		}
		ye.RUnlock()
		ctx, cancel := context.WithCancel(ctx)
		for {

			clusters, err := ye.sdk.GetPostgreSQLClusters(ctx, folderID, f)
			if err != nil {
				log.Errorf("[Yandex.Cloud SD] Failed to get cluster list, error: %v", err)
				select {
				case <-ctx.Done():
					log.Debug("[Yandex.Cloud SD] Context canceled, shutting down Yandex Discovery Engine.")
					cancel()
					return
				default:
					time.Sleep(interval)
					continue
				}
			}

			clustersMap := make(map[hostDb]clusterDSN, len(clusters))
			for _, cluster := range clusters {
				for _, host := range cluster.Hosts {
					for _, database := range cluster.Databases {
						hostDb := hostDb(makeValidMetricName(fmt.Sprintf("%s_%s", host.Name, database.Name)))
						dsn := clusterDSN{
							dsn:  fmt.Sprintf("postgresql://%s:%s@%s:6432/%s", username, password, host.Name, database.Name),
							name: cluster.Name,
						}
						clustersMap[hostDb] = dsn
						if ye.config.TargetLabels != nil {
							dsn.labels = *ye.config.TargetLabels
						}
						clustersMap[hostDb] = dsn
					}
				}
			}

			ye.Lock()
			for hostDb := range maps.Keys(clustersMap) {
				if _, found := ye.dsn[hostDb]; !found {
					ye.dsn[hostDb] = clustersMap[hostDb]
					ye.version++
				}
			}
			for hostDb := range maps.Keys(ye.dsn) {
				if _, found := clustersMap[hostDb]; !found {
					delete(ye.dsn, hostDb)
					ye.version++
				}
			}
			ye.Unlock()
			select {
			case <-ctx.Done():
				log.Debug("[Yandex.Cloud SD] Context canceled, shutting down Yandex Discovery Engine.")
				cancel()
				return
			default:
				time.Sleep(interval)
			}
		}
	}()
	return nil
}

func makeValidMetricName(s string) string {
	var ret = ""
	for i, b := range strings.Replace(s, ".mdb.yandexcloud.net", "", 1) {
		if (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || b == '_' || b == ':' || (b >= '0' && b <= '9' && i > 0) {
			ret += string(b)
		} else {
			ret += "_"
		}
	}
	return ret
}
