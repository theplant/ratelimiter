package redisrl_test

import (
	"context"
	"log"
	"testing"

	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/theplant/ratelimiter/internal/testsupport"
	"github.com/theplant/ratelimiter/redisrl"
)

var redisClient *redis.Client

func TestMain(m *testing.M) {
	ctx := context.Background()
	redisContainer, err := testsupport.OpenRedisContainer(ctx)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := redisContainer.Close(ctx); err != nil {
			log.Printf("Failed to close redis container: %v", err)
		}
	}()

	redisClient = redisContainer.Client
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
