package cache

import (
	"encoding/json"
	"fmt"
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/cherts/pgscv/internal/model"
	"math"
	"time"
)

// MemcachedCache client
type MemcachedCache struct {
	client    *memcache.Client
	gitCommit string
}

// NewMemcachedCache return pointer to MemcachedCache struct
func NewMemcachedCache(addr string, gitCommit string) *MemcachedCache {
	return &MemcachedCache{
		client:    memcache.New(addr),
		gitCommit: gitCommit,
	}
}

// Get value by key
func (c *MemcachedCache) Get(key string) (*model.PGResult, error) {
	item, err := c.client.Get(key)
	if err != nil {
		return nil, err
	}
	var result model.PGResult
	err = json.Unmarshal(item.Value, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// Set value by key with ttl
func (c *MemcachedCache) Set(key string, value *model.PGResult, ttl time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if ttl.Seconds() > math.MaxInt32 {
		return fmt.Errorf("TTL must be between 0 and math.MaxInt32 seconds")
	}
	return c.client.Set(&memcache.Item{
		Key:        key,
		Value:      data,
		Expiration: int32(ttl.Seconds()), // #nosec G115
	})
}

// Delete value by key
func (c *MemcachedCache) Delete(key string) error {
	return c.client.Delete(key)
}

// Hash sha of concatenated string from args
func (c *MemcachedCache) Hash(args ...any) string {
	return hash(c.gitCommit, args)
}
