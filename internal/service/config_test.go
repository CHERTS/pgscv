package service

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_ParsePostgresDSNEnv(t *testing.T) {
	gotID, gotCS, err := ParsePostgresDSNEnv("POSTGRES_DSN", "conninfo")
	assert.NoError(t, err)
	assert.Equal(t, "postgres", gotID)
	assert.Equal(t, ConnSetting{ServiceType: "postgres", Conninfo: "conninfo"}, gotCS)

	gotID, gotCS, err = ParsePostgresDSNEnv("DATABASE_DSN", "conninfo")
	assert.NoError(t, err)
	assert.Equal(t, "postgres", gotID)
	assert.Equal(t, ConnSetting{ServiceType: "postgres", Conninfo: "conninfo"}, gotCS)

	_, _, err = ParsePostgresDSNEnv("INVALID", "conninfo")
	assert.Error(t, err)
}

func Test_ParsePgbouncerDSNEnv(t *testing.T) {
	gotID, gotCS, err := ParsePgbouncerDSNEnv("PGBOUNCER_DSN", "conninfo")
	assert.NoError(t, err)
	assert.Equal(t, "pgbouncer", gotID)
	assert.Equal(t, ConnSetting{ServiceType: "pgbouncer", Conninfo: "conninfo"}, gotCS)

	_, _, err = ParsePgbouncerDSNEnv("INVALID", "conninfo")
	assert.Error(t, err)
}

func Test_parseDSNEnv(t *testing.T) {
	testcases := []struct {
		valid    bool
		prefix   string
		key      string
		wantID   string
		wantType string
	}{
		{valid: true, prefix: "POSTGRES_DSN", key: "POSTGRES_DSN", wantID: "postgres", wantType: "postgres"},
		{valid: true, prefix: "POSTGRES_DSN", key: "POSTGRES_DSN_POSTGRES_123", wantID: "POSTGRES_123", wantType: "postgres"},
		{valid: true, prefix: "POSTGRES_DSN", key: "POSTGRES_DSN1", wantID: "1", wantType: "postgres"},
		{valid: true, prefix: "POSTGRES_DSN", key: "POSTGRES_DSN_POSTGRES_5432", wantID: "POSTGRES_5432", wantType: "postgres"},
		{valid: true, prefix: "PGBOUNCER_DSN", key: "PGBOUNCER_DSN", wantID: "pgbouncer", wantType: "pgbouncer"},
		{valid: true, prefix: "PGBOUNCER_DSN", key: "PGBOUNCER_DSN_PGBOUNCER_123", wantID: "PGBOUNCER_123", wantType: "pgbouncer"},
		{valid: true, prefix: "PGBOUNCER_DSN", key: "PGBOUNCER_DSN1", wantID: "1", wantType: "pgbouncer"},
		{valid: true, prefix: "PGBOUNCER_DSN", key: "PGBOUNCER_DSN_PGBOUNCER_6432", wantID: "PGBOUNCER_6432", wantType: "pgbouncer"},
		{valid: false, prefix: "POSTGRES_DSN", key: "POSTGRES_DSN_"},
		{valid: false, prefix: "POSTGRES_DSN", key: "INVALID"},
		{valid: false, prefix: "INVALID", key: "INVALID"},
	}

	for _, tc := range testcases {
		gotID, gotCS, err := parseDSNEnv(tc.prefix, tc.key, "conninfo")
		if tc.valid {
			assert.NoError(t, err)
			assert.Equal(t, tc.wantID, gotID)
			assert.Equal(t, ConnSetting{ServiceType: tc.wantType, Conninfo: "conninfo"}, gotCS)
		} else {
			assert.Error(t, err)
		}
	}
}

func Test_parseURLEnv(t *testing.T) {
	testcases := []struct {
		valid    bool
		prefix   string
		key      string
		wantID   string
		wantType string
	}{
		{valid: true, prefix: "PATRONI_URL", key: "PATRONI_URL", wantID: "patroni", wantType: "patroni"},
		{valid: true, prefix: "PATRONI_URL", key: "PATRONI_URL1", wantID: "1", wantType: "patroni"},
		{valid: true, prefix: "PATRONI_URL", key: "PATRONI_URL_PATRONI_123", wantID: "PATRONI_123", wantType: "patroni"},
		//
		{valid: false, prefix: "PATRONI_URL", key: "PATRONI_URL_"},
		{valid: false, prefix: "PATRONI_URL", key: "INVALID"},
		{valid: false, prefix: "INVALID", key: "INVALID"},
	}

	for _, tc := range testcases {
		gotID, gotCS, err := parseURLEnv(tc.prefix, tc.key, "baseurl")
		if tc.valid {
			assert.NoError(t, err)
			assert.Equal(t, tc.wantID, gotID)
			assert.Equal(t, ConnSetting{ServiceType: tc.wantType, BaseURL: "baseurl"}, gotCS)
		} else {
			assert.Error(t, err)
		}
	}
}
