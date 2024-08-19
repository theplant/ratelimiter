package ratelimiter

import (
	"context"
	_ "embed"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
)

var errUnexpectedScriptResultFormat = errors.New("ratelimiter: unexpected script result format")

//go:embed embed/redis.lua
var redisScript string

type RedisDriver struct {
	client     *redis.Client
	scriptSha1 string
}

func InitRedisDriver(ctx context.Context, client *redis.Client) (*RedisDriver, error) {
	res, err := client.ScriptLoad(ctx, redisScript).Result()
	if err != nil {
		return nil, errors.Wrap(err, "ratelimiter: failed to load lua script")
	}

	return &RedisDriver{
		client:     client,
		scriptSha1: res,
	}, nil
}

func (d *RedisDriver) Reserve(ctx context.Context, req *ReserveRequest) (*Reservation, error) {
	if req.Key == "" || req.DurationPerToken <= 0 || req.Burst <= 0 || req.Tokens <= 0 || req.Tokens > req.Burst {
		return nil, errors.Wrapf(ErrInvalidParameters, "%v", req)
	}

	select {
	case <-ctx.Done():
		return nil, errors.Wrap(ctx.Err(), "ratelimiter: context done")
	default:
	}

	unixMicroNow := int64(-1)
	if Test {
		nowFunc, exists := NowFuncFromContextForTest(ctx)
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

	result, err := d.client.EvalSha(ctx, d.scriptSha1, []string{req.Key}, args...).Result()
	if err != nil {
		return nil, errors.Wrap(err, "ratelimiter: failed to execute lua script")
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
		return nil, errors.Wrap(ErrInvalidParameters, "lua script")
	}

	return &Reservation{
		ReserveRequest: req,
		OK:             status == 0,
		TimeToAct:      time.UnixMicro(unixMicroToAct).UTC(),
		Now:            time.UnixMicro(unixMicroNow).UTC(),
	}, nil
}
