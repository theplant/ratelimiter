package redisrl

import (
	"context"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"
	"github.com/theplant/ratelimiter"
	"github.com/theplant/testenv"
)

var redisCli *redis.Client

func TestMain(m *testing.M) {
	env, err := testenv.New().RedisEnable(true).SetUp()
	if err != nil {
		panic(err)
	}
	defer env.TearDown()

	redisCli = env.Redis
	m.Run()
}

func TestRedisRateLimiter_New(t *testing.T) {
	ctx := context.Background()
	limiter, err := New(ctx, redisCli)
	require.NoError(t, err)
	require.NotNil(t, limiter)
	require.NotEmpty(t, limiter.scriptSha1)
}

func TestRedisRateLimiter_Reserve(t *testing.T) {
	ctx := ratelimiter.WithTestMode(context.Background())
	limiter, err := New(ctx, redisCli)
	require.NoError(t, err)

	key := "TestRedisReserve"
	durationPerToken := 100 * time.Millisecond
	burst := 3
	now := time.Now()

	// Test successful reservation
	req := &ratelimiter.ReserveRequest{
		Key:              key,
		DurationPerToken: durationPerToken,
		Burst:            burst,
		Tokens:           1,
		MaxFutureReserve: 0,
	}

	testCtx := ratelimiter.WithNowFuncForTest(ctx, func() time.Time {
		return now
	})

	r, err := limiter.Reserve(testCtx, req)
	require.NoError(t, err)
	require.NotNil(t, r)
	require.True(t, r.OK)
	require.Equal(t, req, r.ReserveRequest)

	// Test burst limit
	for i := 0; i < burst-1; i++ {
		r, err := limiter.Reserve(testCtx, req)
		require.NoError(t, err)
		require.True(t, r.OK)
	}

	// Should fail after burst limit
	r, err = limiter.Reserve(testCtx, req)
	require.NoError(t, err)
	require.False(t, r.OK)
}

func TestRedisRateLimiter_Allow(t *testing.T) {
	ctx := ratelimiter.WithTestMode(context.Background())
	limiter, err := New(ctx, redisCli)
	require.NoError(t, err)

	key := "TestRedisAllow"
	req := &ratelimiter.AllowRequest{
		Key:              key,
		DurationPerToken: 100 * time.Millisecond,
		Burst:            2,
		Tokens:           1,
	}

	testCtx := ratelimiter.WithNowFuncForTest(ctx, func() time.Time {
		return time.Now()
	})

	// First request should be allowed
	allowed, err := limiter.Allow(testCtx, req)
	require.NoError(t, err)
	require.True(t, allowed)

	// Second request should be allowed (within burst)
	allowed, err = limiter.Allow(testCtx, req)
	require.NoError(t, err)
	require.True(t, allowed)

	// Third request should be denied (exceeds burst)
	allowed, err = limiter.Allow(testCtx, req)
	require.NoError(t, err)
	require.False(t, allowed)
}

func TestRedisRateLimiter_ValidateRequest(t *testing.T) {
	ctx := context.Background()
	limiter, err := New(ctx, redisCli)
	require.NoError(t, err)

	// Test invalid request
	invalidReq := &ratelimiter.ReserveRequest{
		Key:              "",
		DurationPerToken: 0,
		Burst:            0,
		Tokens:           0,
	}

	r, err := limiter.Reserve(ctx, invalidReq)
	require.Error(t, err)
	require.Nil(t, r)
}

func TestRedisRateLimiter_ContextCancellation(t *testing.T) {
	limiter, err := New(context.Background(), redisCli)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	req := &ratelimiter.ReserveRequest{
		Key:              "TestContextCancel",
		DurationPerToken: 100 * time.Millisecond,
		Burst:            5,
		Tokens:           1,
		MaxFutureReserve: 0,
	}

	r, err := limiter.Reserve(ctx, req)
	require.Error(t, err)
	require.Nil(t, r)
	require.Contains(t, err.Error(), "context done")
}
