package redisrl

import (
	"context"
	_ "embed"
	"time"

	"github.com/pkg/errors"
	redis "github.com/redis/go-redis/v9"
	"github.com/theplant/ratelimiter"
)

var (
	errUnexpectedScriptResultFormat = errors.New("unexpected script result format")
	errLuaScriptFailed              = errors.New("lua script failed")
)

//go:embed embed/redis.lua
var redisScript string

// RateLimiter implements the RateLimiter interface using Redis as the storage backend.
// It uses a Lua script to ensure atomic operations and high performance.
type RateLimiter struct {
	client     *redis.Client
	scriptSha1 string
}

// Ensure RedisRateLimiter implements the RateLimiter interface
var _ ratelimiter.RateLimiter = &RateLimiter{}

// New creates and initializes a new RedisRateLimiter.
// It loads the required Lua script into Redis and returns a configured rate limiter.
// The script SHA1 is cached to avoid reloading on each operation.
func New(ctx context.Context, client *redis.Client) (*RateLimiter, error) {
	res, err := client.ScriptLoad(ctx, redisScript).Result()
	if err != nil {
		return nil, errors.Wrap(err, "failed to load lua script")
	}

	return &RateLimiter{
		client:     client,
		scriptSha1: res,
	}, nil
}

// Reserve attempts to reserve the specified number of tokens.
// It returns a Reservation indicating whether the request was successful
// and when the action should be performed.
func (r *RateLimiter) Reserve(ctx context.Context, req *ratelimiter.ReserveRequest) (*ratelimiter.Reservation, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		return nil, errors.Wrap(ctx.Err(), "context done")
	default:
	}

	unixMicroNow := int64(-1)
	if ratelimiter.IsTestMode(ctx) {
		nowFunc, exists := ratelimiter.NowFuncFromContextForTest(ctx)
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
		return nil, errors.WithStack(errLuaScriptFailed)
	}

	return &ratelimiter.Reservation{
		ReserveRequest: req,
		OK:             status == 0,
		TimeToAct:      time.UnixMicro(unixMicroToAct).UTC(),
		ReservedAt:     time.UnixMicro(unixMicroNow).UTC(),
	}, nil
}
