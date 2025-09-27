package cache

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetCacheClient(t *testing.T) {
	t.Run("in-memory client", func(t *testing.T) {
		config := Config{Type: "in-memory"}
		client := GetCacheClient(config, "test-commit")
		assert.IsType(t, &InMemoryCache{}, client)
	})

	t.Run("memcached client", func(t *testing.T) {
		config := Config{Type: "memcached", Server: "localhost:11211"}
		client := GetCacheClient(config, "test-commit")
		assert.IsType(t, &MemcachedCache{}, client)
	})

	t.Run("unknown type returns nil", func(t *testing.T) {
		config := Config{Type: "unknown"}
		client := GetCacheClient(config, "test-commit")
		assert.Nil(t, client)
	})
}
