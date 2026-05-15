package redisrl_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	testredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/theplant/ratelimiter/redisrl"
)

var redisClient *redis.Client

func TestMain(m *testing.M) {
	ctx := context.Background()
	container, err := testredis.Run(ctx, "redis:7-alpine")
	if err != nil {
		panic(fmt.Errorf("failed to start redis container: %w", err))
	}
	defer func() {
		if err := container.Terminate(context.Background()); err != nil {
			panic(fmt.Errorf("failed to terminate redis container: %w", err))
		}
	}()

	endpoint, err := container.ConnectionString(ctx)
	if err != nil {
		panic(fmt.Errorf("failed to get redis connection string: %w", err))
	}

	redisClient = redis.NewClient(&redis.Options{
		Addr: strings.TrimPrefix(endpoint, "redis://"),
	})
	defer func() { _ = redisClient.Close() }()

	m.Run()
}

// TestNew tests Redis-specific creation scenarios
func TestNew(t *testing.T) {
	ctx := context.Background()

	t.Run("successful creation", func(t *testing.T) {
		limiter, err := redisrl.New(ctx, redisClient)
		require.NoError(t, err)
		require.NotNil(t, limiter)
	})

	t.Run("with disconnected client", func(t *testing.T) {
		// Create a client with invalid address
		invalidClient := redis.NewClient(&redis.Options{
			Addr: "invalid:6379",
		})

		limiter, err := redisrl.New(ctx, invalidClient)
		require.Error(t, err)
		require.Nil(t, limiter)
	})
}
