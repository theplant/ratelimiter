package ratelimiter_test

import (
	"context"
	"testing"
	"time"

	"github.com/theplant/ratelimiter"
	"github.com/theplant/ratelimiter/redisrl"
	"github.com/theplant/ratelimiter/sqlrl"
)

func runBenchmarks(b *testing.B, limiter ratelimiter.RateLimiter) {
	ctx := context.Background()

	tests := []struct {
		key              string
		durationPerToken time.Duration
		burst            int
	}{
		{"Key1_Duration10ms_Burst5", 10 * time.Millisecond, 5},
		{"Key2_Duration20ms_Burst10", 20 * time.Millisecond, 10},
		{"Key3_Duration50ms_Burst3", 50 * time.Millisecond, 3},
	}

	for _, tt := range tests {
		b.Run(tt.key, func(b *testing.B) {
			now := time.Now()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				reserveReq := &ratelimiter.ReserveRequest{
					Key:              tt.key,
					DurationPerToken: tt.durationPerToken,
					Burst:            tt.burst,
					Tokens:           1,
					MaxFutureReserve: 0,
				}
				ctx := ratelimiter.WithNowFuncForTest(ctx, func() time.Time {
					return now.Add(time.Duration(i) * tt.durationPerToken)
				})
				_, err := limiter.Reserve(ctx, reserveReq)
				if err != nil {
					b.Fatalf("failed to reserve: %v", err)
				}
			}
		})
	}
}

func BenchmarkRedisRateLimiter_Reserve(b *testing.B) {
	limiter, err := redisrl.New(context.Background(), redisCli)
	if err != nil {
		b.Fatalf("failed to initialize Redis rate limiter: %v", err)
	}
	runBenchmarks(b, limiter)
}

func BenchmarkSQLRateLimiter_Reserve(b *testing.B) {
	limiter, err := sqlrl.New(db, "kvs")
	if err != nil {
		b.Fatal(err)
	}
	runBenchmarks(b, limiter)
}
