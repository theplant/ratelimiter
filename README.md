# ratelimiter

Currently supports Redis and SQL (GORM) storage backends.

```go
package ratelimiter_test

import (
	"context"
	"fmt"
	"time"

	"github.com/theplant/ratelimiter"
)

func runExample(limiter *ratelimiter.RateLimiter, key string) {
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
			fmt.Printf("%v: allowed: %t\n", delta, r.OK)
			return true
		}

		fmt.Printf("%v: allowed: %t , you can retry after %v\n", delta, false, r.MustRetryAfterFrom(advancedNow))
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


func ExampleNewRedisRateLimiter() {
	limiter, err := ratelimiter.NewRedisRateLimiter(context.Background(), redisCli)
	if err != nil {
		panic(err)
	}
	runExample(limiter, "ExampleNewRedisRateLimiter")
	// Output:
	// 0s: allowed: true
	// 1m0s: allowed: true
	// 2m0s: allowed: true
	// 3m0s: allowed: true
	// 4m0s: allowed: true
	// 5m0s: allowed: false , you can retry after 5m0s
	// 6m0s: allowed: false , you can retry after 4m0s
	// 7m0s: allowed: false , you can retry after 3m0s
	// 8m0s: allowed: false , you can retry after 2m0s
	// 9m0s: allowed: false , you can retry after 1m0s
	// 10m0s: allowed: true
	// 11m0s: allowed: false , you can retry after 9m0s
	// 12m0s: allowed: false , you can retry after 8m0s
	// 13m0s: allowed: false , you can retry after 7m0s
	// 14m0s: allowed: false , you can retry after 6m0s
	// 15m0s: allowed: false , you can retry after 5m0s
	// 16m0s: allowed: false , you can retry after 4m0s
	// 17m0s: allowed: false , you can retry after 3m0s
	// 18m0s: allowed: false , you can retry after 2m0s
	// 19m0s: allowed: false , you can retry after 1m0s
	// 20m0s: allowed: true
	// 21m0s: allowed: false , you can retry after 9m0s
	// 22m0s: allowed: false , you can retry after 8m0s
	// 23m0s: allowed: false , you can retry after 7m0s
	// 24m0s: allowed: false , you can retry after 6m0s
	// --- Sleep 20 minutes ---
	// 45m0s: allowed: true
	// 46m0s: allowed: true
	// 47m0s: allowed: false , you can retry after 3m0s
	// 48m0s: allowed: false , you can retry after 2m0s
	// 49m0s: allowed: false , you can retry after 1m0s
	// 50m0s: allowed: true
	// 51m0s: allowed: false , you can retry after 9m0s
	// 52m0s: allowed: false , you can retry after 8m0s
	// 53m0s: allowed: false , you can retry after 7m0s
	// 54m0s: allowed: false , you can retry after 6m0s
	// --- Sleep 100 minutes ---
	// 2h35m0s: allowed: true
	// 2h36m0s: allowed: true
	// 2h37m0s: allowed: true
	// 2h38m0s: allowed: true
	// 2h39m0s: allowed: true
	// 2h40m0s: allowed: false , you can retry after 5m0s
	// 2h41m0s: allowed: false , you can retry after 4m0s
	// 2h42m0s: allowed: false , you can retry after 3m0s
	// 2h43m0s: allowed: false , you can retry after 2m0s
	// 2h44m0s: allowed: false , you can retry after 1m0s
}

func ExampleNewSQLRateLimiter() {
	// Create SQL rate limiter with custom table name
	limiter, err := ratelimiter.NewSQLRateLimiter(db, "rate_limits")
	if err != nil {
		panic(err)
	}

	// Optional: create table if it doesn't exist
	ctx := context.Background()
	if err := limiter.Migrate(ctx); err != nil {
		panic(err)
	}

	runExample(limiter, "ExampleNewSQLRateLimiter")
}

### Benchmark

```

goos: darwin
goarch: arm64
pkg: github.com/theplant/ratelimiter
cpu: Apple M3 Pro
BenchmarkRedisRateLimiter_Reserve
BenchmarkRedisRateLimiter_Reserve/Key1_Duration10ms_Burst5
BenchmarkRedisRateLimiter_Reserve/Key1_Duration10ms_Burst5-12 4432 268164 ns/op 677 B/op 16 allocs/op
BenchmarkRedisRateLimiter_Reserve/Key2_Duration20ms_Burst10
BenchmarkRedisRateLimiter_Reserve/Key2_Duration20ms_Burst10-12 4605 273537 ns/op 672 B/op 16 allocs/op
BenchmarkRedisRateLimiter_Reserve/Key3_Duration50ms_Burst3
BenchmarkRedisRateLimiter_Reserve/Key3_Duration50ms_Burst3-12 4639 265125 ns/op 672 B/op 16 allocs/op

BenchmarkSQLRateLimiter_Reserve
BenchmarkSQLRateLimiter_Reserve/Key1_Duration10ms_Burst5
BenchmarkSQLRateLimiter_Reserve/Key1_Duration10ms_Burst5-12 1166 943641 ns/op 12959 B/op 166 allocs/op
BenchmarkSQLRateLimiter_Reserve/Key2_Duration20ms_Burst10
BenchmarkSQLRateLimiter_Reserve/Key2_Duration20ms_Burst10-12 1347 908259 ns/op 13079 B/op 166 allocs/op
BenchmarkSQLRateLimiter_Reserve/Key3_Duration50ms_Burst3
BenchmarkSQLRateLimiter_Reserve/Key3_Duration50ms_Burst3-12 1357 934328 ns/op 13019 B/op 166 allocs/op

```

```
