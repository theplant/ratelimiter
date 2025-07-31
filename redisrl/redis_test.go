package redisrl_test

import (
	"context"
	"log"
	"testing"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"
	"github.com/theplant/ratelimiter/redisrl"
	"github.com/theplant/testenv"
)

var redisClient *redis.Client

func TestMain(m *testing.M) {
	env, err := testenv.New().RedisEnable(true).SetUp()
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := env.TearDown(); err != nil {
			log.Fatalf("Failed to tear down test environment: %v", err)
		}
	}()

	redisClient = env.Redis
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
