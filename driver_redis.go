package ratelimiter

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
)

var errUnexpectedScriptResultFormat = errors.New("ratelimiter: unexpected script result format")

const redisScript = `
local key = KEYS[1]
local durationPerToken = tonumber(ARGV[1]) -- The time interval required for each token, in microseconds
local burst = tonumber(ARGV[2]) -- Burst capacity
local tokens = tonumber(ARGV[3]) -- Number of tokens requested
local now = tonumber(ARGV[4]) -- Current timestamp, in microseconds
local maxFutureReserve = tonumber(ARGV[5]) -- Maximum reservation duration, in microseconds

if now <= 0 or durationPerToken <= 0 or burst <= 0 or tokens <= 0 or tokens > burst then
	return {-2, 0} -- Indicates invalid parameters
end

-- Calculate the reset value based on the current time and burst duration
local resetValue = now - (burst * durationPerToken)

-- Attempt to get timeBase
local timeBase = tonumber(redis.call("get", key))

-- If timeBase does not exist or is less than the calculated reset value, update it to the reset value
if not timeBase or timeBase < resetValue then
	timeBase = resetValue
end

-- Calculate the time length occupied by the tokens
local tokensDuration = tokens * durationPerToken
local timeToAct = timeBase + tokensDuration

-- If timeToAct exceeds the maximum reservation timeout, do not update timeBase and return an error
if timeToAct > now + maxFutureReserve then
	return {-1, timeToAct} -- Error indicator and returns timeToAct
else
	-- Update timeBase to the execution time of the next request
	redis.call("set", key, timeToAct)
	-- Return the time point when the next action should be performed
	return {0, timeToAct} -- Success indicator and returns timeToAct
end
`

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
	now := req.Now.UTC() // stripMono
	if req.Key == "" || now.IsZero() || req.DurationPerToken <= 0 || req.Burst <= 0 || req.Tokens <= 0 || req.Tokens > req.Burst {
		return nil, errors.Wrapf(ErrInvalidParameters, "%v", req)
	}

	select {
	case <-ctx.Done():
		return nil, errors.Wrap(ctx.Err(), "ratelimiter: context done")
	default:
	}

	args := []any{
		req.DurationPerToken.Microseconds(),
		req.Burst,
		req.Tokens,
		now.UnixMicro(),
		req.MaxFutureReserve.Microseconds(),
	}

	result, err := d.client.EvalSha(ctx, d.scriptSha1, []string{req.Key}, args...).Result()
	if err != nil {
		return nil, errors.Wrap(err, "ratelimiter: failed to execute lua script")
	}

	res, ok := result.([]any)
	if !ok || len(res) != 2 {
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

	if status == -2 {
		return nil, errors.Wrap(ErrInvalidParameters, "lua script")
	}

	return &Reservation{
		ReserveRequest: req,
		OK:             status == 0,
		TimeToAct:      time.UnixMicro(unixMicroToAct).UTC(),
	}, nil
}
