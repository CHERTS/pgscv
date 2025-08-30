package pgscv

import (
	"github.com/cherts/pgscv/internal/cache"
	"github.com/cherts/pgscv/internal/log"
	"github.com/go-playground/validator/v10"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	poolConfigValidator = "pool_config"
)

func registerCustomValidators(v *validator.Validate) {
	_ = v.RegisterValidation(poolConfigValidator, func(fl validator.FieldLevel) bool {
		poolConfig, ok := fl.Field().Interface().(PoolConfig)
		if !ok {
			return false
		}

		defaults, err := pgxpool.ParseConfig("postgres://127.0.0.1")
		if err != nil {
			log.Error("failed parsing pgx connString for validator defaults")
			return false
		}

		var minConns int32
		if poolConfig.MinConns != nil {
			minConns = *poolConfig.MinConns
		} else {
			minConns = defaults.MinConns
		}

		var maxConns int32
		if poolConfig.MaxConns != nil {
			maxConns = *poolConfig.MaxConns
		} else {
			maxConns = defaults.MaxConns
		}

		var minIdleConns int32
		if poolConfig.MinIdleConns != nil {
			minIdleConns = *poolConfig.MinIdleConns
		} else {
			minIdleConns = defaults.MinIdleConns
		}

		if maxConns <= 0 {
			return false
		}

		if minConns < 0 {
			return false
		}

		if minIdleConns < 0 {
			return false
		}

		if minConns > maxConns {
			return false
		}

		if minIdleConns > maxConns {
			return false
		}

		if minIdleConns > minConns {
			return false
		}
		return true
	})
	cache.RegisterValidators(v)
}
