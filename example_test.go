package ratelimiter_test

import (
	"context"
	"fmt"
	"time"

	"github.com/theplant/ratelimiter"
	"github.com/theplant/ratelimiter/redisrl"
	"github.com/theplant/ratelimiter/sqlrl"
)

func runExample(limiter ratelimiter.RateLimiter, key string) {
	// every 10 min , burst 5
	durationPerToken := 10 * time.Minute
	burst := 5
	now := time.Now()

	ctx := context.Background()

	try := func(delta time.Duration) bool {
		reserveReq := &ratelimiter.ReserveRequest{
			Key:              key,
			DurationPerToken: durationPerToken,
			Burst:            burst,
			Tokens:           1,
			MaxFutureReserve: 0,
		}
		advancedNow := now.Add(delta)
		r, err := limiter.Reserve(
			// only for test, you should not use this in production !!
			ratelimiter.WithNowFuncForTest(ctx, func() time.Time {
				return advancedNow
			}),
			reserveReq,
		)
		if err != nil {
			panic(err)
		}

		if r.OK {
			delay := r.MustDelayFrom(advancedNow)
			if delay == 0 {
				fmt.Printf("%v: allowed, act now\n", delta)
			} else {
				fmt.Printf("%v: allowed, act in %v\n", delta, delay)
			}
			return true
		}

		retryAfter := r.MustRetryAfterFrom(advancedNow)
		fmt.Printf("%v: denied, retry in %v\n", delta, retryAfter)
		return false
	}

	for i := 0; i < int(25); i++ {
		delta := time.Duration(i) * time.Minute
		try(delta)
	}

	fmt.Printf("--- Sleep 20 minutes ---\n")

	for i := 45; i < int(55); i++ {
		delta := time.Duration(i) * time.Minute
		try(delta)
	}

	fmt.Printf("--- Sleep 100 minutes ---\n")

	for i := 155; i < int(165); i++ {
		delta := time.Duration(i) * time.Minute
		try(delta)
	}
}

func Example_sqlRateLimiter() {
	// Create SQL rate limiter with custom table name
	limiter, err := sqlrl.New(db, "example_rate_limits")
	if err != nil {
		panic(err)
	}

	// Optional: create table if it doesn't exist
	ctx := context.Background()
	if err := limiter.Migrate(ctx); err != nil {
		panic(err)
	}

	runExample(limiter, "Example_sqlRateLimiter")
	// Output:
	// 0s: allowed, act now
	// 1m0s: allowed, act now
	// 2m0s: allowed, act now
	// 3m0s: allowed, act now
	// 4m0s: allowed, act now
	// 5m0s: denied, retry in 5m0s
	// 6m0s: denied, retry in 4m0s
	// 7m0s: denied, retry in 3m0s
	// 8m0s: denied, retry in 2m0s
	// 9m0s: denied, retry in 1m0s
	// 10m0s: allowed, act now
	// 11m0s: denied, retry in 9m0s
	// 12m0s: denied, retry in 8m0s
	// 13m0s: denied, retry in 7m0s
	// 14m0s: denied, retry in 6m0s
	// 15m0s: denied, retry in 5m0s
	// 16m0s: denied, retry in 4m0s
	// 17m0s: denied, retry in 3m0s
	// 18m0s: denied, retry in 2m0s
	// 19m0s: denied, retry in 1m0s
	// 20m0s: allowed, act now
	// 21m0s: denied, retry in 9m0s
	// 22m0s: denied, retry in 8m0s
	// 23m0s: denied, retry in 7m0s
	// 24m0s: denied, retry in 6m0s
	// --- Sleep 20 minutes ---
	// 45m0s: allowed, act now
	// 46m0s: allowed, act now
	// 47m0s: denied, retry in 3m0s
	// 48m0s: denied, retry in 2m0s
	// 49m0s: denied, retry in 1m0s
	// 50m0s: allowed, act now
	// 51m0s: denied, retry in 9m0s
	// 52m0s: denied, retry in 8m0s
	// 53m0s: denied, retry in 7m0s
	// 54m0s: denied, retry in 6m0s
	// --- Sleep 100 minutes ---
	// 2h35m0s: allowed, act now
	// 2h36m0s: allowed, act now
	// 2h37m0s: allowed, act now
	// 2h38m0s: allowed, act now
	// 2h39m0s: allowed, act now
	// 2h40m0s: denied, retry in 5m0s
	// 2h41m0s: denied, retry in 4m0s
	// 2h42m0s: denied, retry in 3m0s
	// 2h43m0s: denied, retry in 2m0s
	// 2h44m0s: denied, retry in 1m0s
}

func Example_redisRateLimiter() {
	limiter, err := redisrl.New(context.Background(), redisCli)
	if err != nil {
		panic(err)
	}
	runExample(limiter, "Example_redisRateLimiter")
	// Output:
	// 0s: allowed, act now
	// 1m0s: allowed, act now
	// 2m0s: allowed, act now
	// 3m0s: allowed, act now
	// 4m0s: allowed, act now
	// 5m0s: denied, retry in 5m0s
	// 6m0s: denied, retry in 4m0s
	// 7m0s: denied, retry in 3m0s
	// 8m0s: denied, retry in 2m0s
	// 9m0s: denied, retry in 1m0s
	// 10m0s: allowed, act now
	// 11m0s: denied, retry in 9m0s
	// 12m0s: denied, retry in 8m0s
	// 13m0s: denied, retry in 7m0s
	// 14m0s: denied, retry in 6m0s
	// 15m0s: denied, retry in 5m0s
	// 16m0s: denied, retry in 4m0s
	// 17m0s: denied, retry in 3m0s
	// 18m0s: denied, retry in 2m0s
	// 19m0s: denied, retry in 1m0s
	// 20m0s: allowed, act now
	// 21m0s: denied, retry in 9m0s
	// 22m0s: denied, retry in 8m0s
	// 23m0s: denied, retry in 7m0s
	// 24m0s: denied, retry in 6m0s
	// --- Sleep 20 minutes ---
	// 45m0s: allowed, act now
	// 46m0s: allowed, act now
	// 47m0s: denied, retry in 3m0s
	// 48m0s: denied, retry in 2m0s
	// 49m0s: denied, retry in 1m0s
	// 50m0s: allowed, act now
	// 51m0s: denied, retry in 9m0s
	// 52m0s: denied, retry in 8m0s
	// 53m0s: denied, retry in 7m0s
	// 54m0s: denied, retry in 6m0s
	// --- Sleep 100 minutes ---
	// 2h35m0s: allowed, act now
	// 2h36m0s: allowed, act now
	// 2h37m0s: allowed, act now
	// 2h38m0s: allowed, act now
	// 2h39m0s: allowed, act now
	// 2h40m0s: denied, retry in 5m0s
	// 2h41m0s: denied, retry in 4m0s
	// 2h42m0s: denied, retry in 3m0s
	// 2h43m0s: denied, retry in 2m0s
	// 2h44m0s: denied, retry in 1m0s
}
