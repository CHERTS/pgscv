package cache

// GetCacheClient - Instantiate cache
func GetCacheClient(config Config, gitCommit string) Cache {
	switch config.Type {
	case cacheInMemory:
		return NewInMemoryCache()
	case cacheMemcached:
		return NewMemcachedCache(config.Server, gitCommit)
	}
	return nil
}
