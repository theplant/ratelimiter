package ratelimiter

import (
	"context"
	_ "embed"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
)

var (
	errUnexpectedScriptResultFormat = errors.New("unexpected script result format")
	errLuaScriptFailed              = errors.New("lua script failed")
)

//go:embed embed/redis.lua
var redisScript string

// RedisRateLimiter implements the RateLimiter interface using Redis as the storage backend.
// It uses a Lua script to ensure atomic operations and high performance.
type RedisRateLimiter struct {
	client     *redis.Client
	scriptSha1 string
}

// Ensure RedisRateLimiter implements the RateLimiter interface
var _ RateLimiter = &RedisRateLimiter{}

// NewRedisRateLimiter creates and initializes a new RedisRateLimiter.
// It loads the required Lua script into Redis and returns a configured rate limiter.
// The script SHA1 is cached to avoid reloading on each operation.
func NewRedisRateLimiter(ctx context.Context, client *redis.Client) (*RedisRateLimiter, error) {
	res, err := client.ScriptLoad(ctx, redisScript).Result()
	if err != nil {
		return nil, errors.Wrap(err, "failed to load lua script")
	}

	return &RedisRateLimiter{
		client:     client,
		scriptSha1: res,
	}, nil
}

// Allow checks if the specified number of tokens are available immediately.
// It returns true if the request can be satisfied without waiting, false otherwise.
// This method is implemented as a Reserve operation with MaxFutureReserve set to 0.
func (r *RedisRateLimiter) Allow(ctx context.Context, req *AllowRequest) (bool, error) {
	return Allow(ctx, r, req)
}

// Reserve attempts to reserve the specified number of tokens.
// It returns a Reservation indicating whether the request was successful
// and when the action should be performed.
func (r *RedisRateLimiter) Reserve(ctx context.Context, req *ReserveRequest) (*Reservation, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		return nil, errors.Wrap(ctx.Err(), "context done")
	default:
	}

	unixMicroNow := int64(-1)
	if isTestMode(ctx) {
		nowFunc, exists := nowFuncFromContextForTest(ctx)
		if exists {
			unixMicroNow = nowFunc().UTC().UnixMicro() // stripMono
		}
	}

	args := []any{
		req.DurationPerToken.Microseconds(),
		req.Burst,
		req.Tokens,
		unixMicroNow,
		req.MaxFutureReserve.Microseconds(),
	}

	result, err := r.client.EvalSha(ctx, r.scriptSha1, []string{req.Key}, args...).Result()
	if err != nil {
		return nil, errors.Wrap(err, "failed to execute lua script")
	}

	res, ok := result.([]any)
	if !ok || len(res) != 3 {
		return nil, errors.Wrap(errUnexpectedScriptResultFormat, "length of result")
	}
	status, ok := res[0].(int64)
	if !ok {
		return nil, errors.Wrap(errUnexpectedScriptResultFormat, "status")
	}
	unixMicroToAct, ok := res[1].(int64)
	if !ok {
		return nil, errors.Wrap(errUnexpectedScriptResultFormat, "unixMicroToAct")
	}
	unixMicroNow, ok = res[2].(int64)
	if !ok {
		return nil, errors.Wrap(errUnexpectedScriptResultFormat, "unixMicroNow")
	}
	if status == -2 {
		return nil, errLuaScriptFailed
	}

	return &Reservation{
		ReserveRequest: req,
		OK:             status == 0,
		TimeToAct:      time.UnixMicro(unixMicroToAct).UTC(),
		ReservedAt:     time.UnixMicro(unixMicroNow).UTC(),
	}, nil
}
