// Package testsupport provides test-only container helpers for the
// ratelimiter repo. It exists so the migration off
// github.com/theplant/testenv (which still pins github.com/docker/docker)
// did not need to leak its replacement into every _test.go.
package testsupport

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	testredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

type RedisContainer struct {
	testcontainers.Container
	Client *redis.Client
}

func (c *RedisContainer) Close(ctx context.Context) error {
	var clientErr error
	if c.Client != nil {
		clientErr = c.Client.Close()
	}
	if err := c.Container.Terminate(ctx); err != nil {
		return err
	}
	return clientErr
}

// OpenRedisContainer starts a Redis test container and returns a ready
// *redis.Client. Call Close on the returned container to stop and clean
// up. The implementation uses testcontainers-go's redis module directly,
// avoiding any dependency on github.com/docker/docker.
func OpenRedisContainer(ctx context.Context) (*RedisContainer, error) {
	container, err := testredis.Run(ctx, "redis:8.0-M04-alpine")
	if err != nil {
		return nil, fmt.Errorf("fail to start redis container: %w", err)
	}

	endpoint, err := container.ConnectionString(ctx)
	if err != nil {
		_ = container.Terminate(ctx)
		return nil, fmt.Errorf("fail to get redis endpoint: %w", err)
	}

	opts, err := redis.ParseURL(endpoint)
	if err != nil {
		_ = container.Terminate(ctx)
		return nil, fmt.Errorf("fail to parse redis endpoint: %w", err)
	}

	return &RedisContainer{
		Container: container,
		Client:    redis.NewClient(opts),
	}, nil
}
