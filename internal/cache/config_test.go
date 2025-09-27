package cache

import (
	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestConfigValidation(t *testing.T) {
	v := validator.New()
	RegisterValidators(v)

	tests := []struct {
		name     string
		config   Config
		expected bool
	}{
		{
			name: "valid in-memory config",
			config: Config{
				Type: "in-memory",
				TTL:  "30s",
			},
			expected: true,
		},
		{
			name: "valid memcached config",
			config: Config{
				Type:   "memcached",
				Server: "localhost:11211",
				TTL:    "1m",
			},
			expected: true,
		},
		{
			name: "invalid cache type",
			config: Config{
				Type: "invalid-type",
				TTL:  "30s",
			},
			expected: false,
		},
		{
			name: "memcached without server",
			config: Config{
				Type: "memcached",
				TTL:  "30s",
			},
			expected: false,
		},
		{
			name: "invalid memcached server format",
			config: Config{
				Type:   "memcached",
				Server: "localhost",
				TTL:    "30s",
			},
			expected: false,
		},
		{
			name: "invalid memcached server port",
			config: Config{
				Type:   "memcached",
				Server: "localhost:99999",
				TTL:    "30s",
			},
			expected: false,
		},
		{
			name: "multiple memcached servers",
			config: Config{
				Type:   "memcached",
				Server: "server1:11211,server2:11211",
				TTL:    "30s",
			},
			expected: true,
		},
		{
			name: "empty TTL",
			config: Config{
				Type: "in-memory",
				TTL:  "",
			},
			expected: false,
		},
		{
			name: "invalid TTL format",
			config: Config{
				Type: "in-memory",
				TTL:  "invalid",
			},
			expected: false,
		},
		{
			name: "negative TTL seconds",
			config: Config{
				Type: "in-memory",
				TTL:  "-10s",
			},
			expected: false,
		},
		{
			name: "zero TTL seconds",
			config: Config{
				Type: "in-memory",
				TTL:  "0s",
			},
			expected: false,
		},
		{
			name: "TTL as seconds number",
			config: Config{
				Type: "in-memory",
				TTL:  "60",
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := v.Struct(tt.config)
			if tt.expected {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestTTLSecondsConversion(t *testing.T) {
	tests := []struct {
		name     string
		ttl      string
		expected int32
		hasError bool
	}{
		{"duration seconds", "30s", 30, false},
		{"duration minutes", "1m", 60, false},
		{"numeric seconds", "45", 45, false},
		{"empty string", "", 0, false},
		{"invalid format", "invalid", 0, true},
		{"negative duration", "-10s", 0, true},
		{"zero duration", "0s", 0, true},
		{"too large duration", "10000000000s", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ttlSeconds(tt.ttl)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestGetCollectorTTL(t *testing.T) {
	config := Config{
		Type: "in-memory",
		TTL:  "60s",
		Collectors: map[string]CollectorConfig{
			"custom": {TTL: "30s"},
		},
	}

	t.Run("custom collector TTL", func(t *testing.T) {
		ttl, err := config.GetCollectorTTL("custom")
		assert.NoError(t, err)
		assert.Equal(t, int32(30), ttl)
	})

	t.Run("default collector TTL", func(t *testing.T) {
		ttl, err := config.GetCollectorTTL("default")
		assert.NoError(t, err)
		assert.Equal(t, int32(60), ttl)
	})

	t.Run("non-existent collector", func(t *testing.T) {
		ttl, err := config.GetCollectorTTL("nonexistent")
		assert.NoError(t, err)
		assert.Equal(t, int32(60), ttl)
	})
}

func TestCollectorConfigValidation(t *testing.T) {
	v := validator.New()
	RegisterValidators(v)

	tests := []struct {
		name     string
		config   CollectorConfig
		expected bool
	}{
		{"valid TTL", CollectorConfig{TTL: "30s"}, true},
		{"empty TTL", CollectorConfig{TTL: ""}, false},
		{"invalid TTL", CollectorConfig{TTL: "invalid"}, false},
		{"numeric TTL", CollectorConfig{TTL: "45"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := v.Struct(tt.config)
			if tt.expected {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}
