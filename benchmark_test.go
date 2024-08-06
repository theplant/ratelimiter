package ratelimiter_test

import (
	"context"
	"testing"
	"time"

	"github.com/theplant/ratelimiter"
)

func runBenchmarks(b *testing.B, limiter *ratelimiter.RateLimiter) {
	ctx := context.Background()

	tests := []struct {
		name             string
		key              string
		durationPerToken time.Duration
		burst            int
	}{
		{"Key1_Duration10ms_Burst5", "BenchmarkRedisDriver_Reserve_1", 10 * time.Millisecond, 5},
		{"Key2_Duration20ms_Burst10", "BenchmarkRedisDriver_Reserve_2", 20 * time.Millisecond, 10},
		{"Key3_Duration50ms_Burst3", "BenchmarkRedisDriver_Reserve_3", 50 * time.Millisecond, 3},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			now := time.Now()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				reserveReq := &ratelimiter.ReserveRequest{
					Key:              tt.key,
					DurationPerToken: tt.durationPerToken,
					Burst:            tt.burst,
					Now:              now.Add(time.Duration(i) * tt.durationPerToken),
					Tokens:           1,
					MaxFutureReserve: 0,
				}
				_, err := limiter.Reserve(ctx, reserveReq)
				if err != nil {
					b.Fatalf("failed to reserve: %v", err)
				}
			}
		})
	}
}

func BenchmarkDriverRedis_Reserve(b *testing.B) {
	driver, err := ratelimiter.InitRedisDriver(context.Background(), redisCli)
	if err != nil {
		b.Fatalf("failed to initialize Redis driver: %v", err)
	}
	limiter := ratelimiter.New(driver)
	runBenchmarks(b, limiter)
}

func BenchmarkDriverGORM_Reserve(b *testing.B) {
	limiter := ratelimiter.New(ratelimiter.DriverGORM(db))
	runBenchmarks(b, limiter)
}
