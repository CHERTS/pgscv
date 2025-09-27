package cache

import (
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/cherts/pgscv/internal/model"
	"sync"
	"time"
)

type cacheItem struct {
	Result *model.PGResult
	TS     time.Time
}

// InMemoryCache - in memory cache
type InMemoryCache struct {
	items map[string]*cacheItem
	mu    sync.RWMutex
}

// NewInMemoryCache return pointer to InMemoryCache
func NewInMemoryCache() *InMemoryCache {
	return &InMemoryCache{
		items: make(map[string]*cacheItem),
	}
}

// Get value by key
func (c *InMemoryCache) Get(key string) (*model.PGResult, time.Time, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	data, ok := c.items[key]
	if !ok {
		return nil, time.Now(), memcache.ErrCacheMiss
	}
	return data.Result, data.TS, nil
}

// Set value with key and ttl
func (c *InMemoryCache) Set(key string, value *model.PGResult, ttl time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items[key] = &cacheItem{Result: value, TS: time.Now()}
	if ttl > 0 {
		go func() {
			time.Sleep(ttl)
			err := c.Delete(key)
			if err != nil {
				return
			}
		}()
	}
	return nil
}

// Delete value from cache with key
func (c *InMemoryCache) Delete(key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.items, key)
	return nil
}

// Hash sha of concatenated string from args
func (c *InMemoryCache) Hash(args ...any) string {
	return hash(args)
}
