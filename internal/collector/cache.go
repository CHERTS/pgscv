package collector

import (
	"errors"
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/cherts/pgscv/internal/cache"
	"github.com/cherts/pgscv/internal/log"
	"github.com/cherts/pgscv/internal/model"
	"sync"
	"time"
)

func getFromCache(cacheConfig *cache.Config, args ...any) (string, *model.PGResult, *time.Time) {
	now := time.Now()

	if cacheConfig == nil {
		return "", nil, &now
	}
	cacheKey := cacheConfig.Cache.Hash(args)
	if cacheKey == "" {
		return "", nil, &now
	}
	res, ts, err := cacheConfig.Cache.Get(cacheKey)
	if err != nil && !errors.Is(err, memcache.ErrCacheMiss) {
		log.Errorf("failed to fetch from cache, err: %v", err)
		return "", nil, &now
	}
	if err == nil {
		return cacheKey, res, &ts
	}
	return cacheKey, nil, &now
}

func saveToCache(collector string, wg *sync.WaitGroup, cacheConfig *cache.Config, cacheKey string, res *model.PGResult) {
	if cacheConfig == nil || cacheKey == "" || cacheConfig.Cache == nil {
		return
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		ttl, err := cacheConfig.GetCollectorTTL(collector)
		if err != nil {
			ttl = 60
		}
		err = cacheConfig.Cache.Set(cacheKey, res, time.Duration(ttl)*time.Second)
		if err != nil {
			return
		}
	}()
}
