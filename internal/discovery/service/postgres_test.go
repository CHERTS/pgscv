package service

import (
	"context"
	"fmt"
	"github.com/cherts/pgscv/discovery"
	"github.com/cherts/pgscv/internal/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

const TestPostgresConnStr = store.TestPostgresConnStr

func TestNewPostgresDiscovery(t *testing.T) {
	pd := NewPostgresDiscovery("test-id")
	assert.NotNil(t, pd)
	assert.NotNil(t, pd.subscribers)
	assert.Empty(t, pd.subscribers)
}

func TestPostgresDiscovery_Init_WithPasswordFromEnv(t *testing.T) {
	pd := NewPostgresDiscovery("test-id")

	envVar := "TEST_PG_PASSWORD"
	testPassword := "testpassword123"
	err := os.Setenv(envVar, testPassword)
	assert.NoError(t, err)

	defer func(key string) {
		err := os.Unsetenv(key)
		assert.NoError(t, err)
	}(envVar)

	config := postgresConfig{
		ConnInfo:        TestPostgresConnStr,
		PasswordFromEnv: &envVar,
		RefreshInterval: 5,
	}

	err = pd.Init(config)
	assert.NoError(t, err)
	assert.Equal(t, testPassword, pd.config.password)
}

func TestPostgresDiscovery_Subscribe_Unsubscribe(t *testing.T) {
	pd := NewPostgresDiscovery("test-id")

	config := postgresConfig{
		ConnInfo: TestPostgresConnStr,
	}
	err := pd.Init(config)
	require.NoError(t, err)

	subscriberID := "test-subscriber"

	addFunc := func(_ map[string]discovery.Service) error {
		return nil
	}

	removeFunc := func(_ []string) error {
		return nil
	}

	err = pd.Subscribe(subscriberID, addFunc, removeFunc)
	assert.NoError(t, err)

	pd.RLock()
	assert.Contains(t, pd.subscribers, subscriberID)
	pd.RUnlock()

	err = pd.Unsubscribe(subscriberID)
	assert.NoError(t, err)

	pd.RLock()
	assert.NotContains(t, pd.subscribers, subscriberID)
	pd.RUnlock()
}

func TestPostgresDiscovery_EnsureDB(t *testing.T) {
	pd := NewPostgresDiscovery("test-id")

	config := postgresConfig{
		ConnInfo: TestPostgresConnStr,
	}
	err := pd.Init(config)
	require.NoError(t, err)

	err = pd.ensureDB()
	assert.NoError(t, err)
	assert.NotNil(t, pd.db)
	assert.NotNil(t, pd.dbConfig)

	originalDB := pd.db
	err = pd.ensureDB()
	assert.NoError(t, err)
	assert.Equal(t, originalDB, pd.db)
}

func TestPostgresDiscovery_GetServices(t *testing.T) {
	pd := NewPostgresDiscovery("test-id")

	config := postgresConfig{
		ConnInfo: TestPostgresConnStr,
	}
	err := pd.Init(config)
	require.NoError(t, err)

	testDBs := []string{"pgscv_fixtures", "postgres"}
	err = pd.ensureDB()
	assert.NoError(t, err)
	services := pd.getServices(testDBs)
	assert.NotNil(t, services)
	assert.Len(t, *services, 2)

	for _, db := range testDBs {
		expectedSvcID := fmt.Sprintf("%s_%s", "test-id", db)
		assert.Contains(t, *services, expectedSvcID)
	}

	for _, service := range *services {
		assert.Contains(t, service.dsn, "postgres://")
		assert.Contains(t, service.dsn, "127.0.0.1:5432")
	}
}

func TestPostgresDiscovery_GetServices_WithFilter(t *testing.T) {
	pd := NewPostgresDiscovery("test-id")

	excludeDB := "postgres"
	config := postgresConfig{
		ConnInfo:  TestPostgresConnStr,
		ExcludeDb: &excludeDB,
	}
	err := pd.Init(config)
	require.NoError(t, err)
	err = pd.ensureDB()
	assert.NoError(t, err)

	testDBs := []string{"pgscv_fixtures", "postgres", "template1"}

	services := pd.getServices(testDBs)
	assert.NotNil(t, services)

	assert.Len(t, *services, 2)
	assert.Contains(t, *services, "test-id_pgscv_fixtures")
	assert.Contains(t, *services, "test-id_template1")
	assert.NotContains(t, *services, "test-id_postgres")
}

func TestPostgresDiscovery_Sync_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pd := NewPostgresDiscovery("test-id")

	config := postgresConfig{
		ConnInfo: TestPostgresConnStr,
	}
	err := pd.Init(config)
	require.NoError(t, err)

	subscriberID := "test-sync-subscriber"
	var receivedServices map[string]discovery.Service

	addFunc := func(services map[string]discovery.Service) error {
		receivedServices = services
		return nil
	}

	removeFunc := func(_ []string) error {
		return nil
	}

	err = pd.Subscribe(subscriberID, addFunc, removeFunc)
	require.NoError(t, err)

	ctx := context.Background()
	err = pd.sync(ctx)
	assert.NoError(t, err)

	assert.NotNil(t, receivedServices)
	assert.True(t, len(receivedServices) > 0)

	for svcID, service := range receivedServices {
		assert.Contains(t, svcID, "test-id_")
		assert.Contains(t, service.DSN, "postgres://")
		assert.Contains(t, service.DSN, "127.0.0.1:5432")
	}
}
