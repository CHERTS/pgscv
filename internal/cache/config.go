// Package cache implement cache of PGResult
package cache

import (
	"fmt"
	"github.com/cherts/pgscv/internal/model"
	"github.com/go-playground/validator/v10"
	"math"
	"strconv"
	"strings"
	"time"
)

const (
	cacheInMemory             = "in-memory"
	cacheMemcached            = "memcached"
	memcachedServersValidator = "memcached_servers"
	ttlValidator              = "ttl"
)

// Config is part of global pgSCV config
type Config struct {
	Type       string                     `yaml:"type" validate:"oneof=in-memory memcached"`
	Server     string                     `yaml:"server" validate:"required_if=Type memcached,omitempty,memcached_servers"`
	TTL        string                     `yaml:"ttl" validate:"required,ttl"`
	Collectors map[string]CollectorConfig `yaml:"collectors"`
	Cache      Cache
}

// CollectorConfig custom ttl for collectors
type CollectorConfig struct {
	TTL string `yaml:"ttl" validate:"required,ttl"`
}

// Cache abstract interface
type Cache interface {
	Get(key string) (*model.PGResult, time.Time, error)
	Set(key string, value *model.PGResult, ttl time.Duration) error
	Delete(key string) error
	Hash(args ...any) string
}

// RegisterValidators Config struct
func RegisterValidators(v *validator.Validate) {
	registerCustomValidators(v)
}

func registerCustomValidators(v *validator.Validate) {
	_ = v.RegisterValidation(memcachedServersValidator, func(fl validator.FieldLevel) bool {
		servers := fl.Field().String()
		if servers == "" {
			return false
		}

		for s := range strings.SplitSeq(servers, ",") {
			parts := make([]string, 0, 2)
			for p := range strings.SplitSeq(s, ":") {
				p = strings.Trim(p, " ")
				parts = append(parts, p)
			}
			if len(parts) != 2 {
				return false
			}
			port, err := strconv.Atoi(parts[1])
			if err != nil || port < 1 || port > 65535 {
				return false
			}
		}
		return true
	})

	_ = v.RegisterValidation(ttlValidator, func(fl validator.FieldLevel) bool {
		ttlStr := fl.Field().String()

		if ttlStr == "" {
			return false
		}

		duration, err := time.ParseDuration(ttlStr)
		if err != nil {
			if seconds, err := strconv.Atoi(ttlStr); err == nil {
				return seconds > 0
			}
			return false
		}
		return duration > 0
	})
}

func (c *Config) getTTLSeconds() (int32, error) {
	return ttlSeconds(c.TTL)
}

func (c *CollectorConfig) getTTLSeconds() (int32, error) {
	return ttlSeconds(c.TTL)
}

func ttlSeconds(ttl string) (int32, error) {
	if ttl == "" {
		return 0, nil
	}

	if duration, err := time.ParseDuration(ttl); err == nil {
		if duration.Seconds() > math.MaxInt32 || duration.Seconds() < 1 {
			return 0, fmt.Errorf("TTL must be between 1 and math.MaxInt32 seconds")
		}
		return int32(duration.Seconds()), nil
	}
	if seconds, err := strconv.Atoi(ttl); err == nil {
		if seconds > math.MaxInt32 || seconds < 1 {
			return 0, fmt.Errorf("TTL must be between 1 and math.MaxInt32 seconds")
		}
		return int32(seconds), nil // #nosec G109 G115
	}
	return 0, fmt.Errorf("invalid TTL format: %s", ttl)
}

// GetCollectorTTL get custom or global ttl for collector
func (c *Config) GetCollectorTTL(collector string) (int32, error) {
	if coll, exists := c.Collectors[collector]; exists {
		return coll.getTTLSeconds()
	}
	return c.getTTLSeconds()
}

func (c *Config) String() string {
	ttl := fmt.Sprintf("default ttl %v", c.TTL)
	if c.Collectors != nil {
		for k, v := range c.Collectors {
			ttl += fmt.Sprintf(", %s:%s", k, v.TTL)
		}
	}
	ret := fmt.Sprintf("type: %s", c.Type)
	if c.Server != "" {
		ret += fmt.Sprintf(", server: %s", c.Server)
	}
	return ret + " " + ttl
}
