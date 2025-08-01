# ratelimiter

A distributed rate limiter library for Go applications.

Currently supports Redis and SQL (GORM) storage backends.

## Usage Examples

For complete runnable examples, see [example_test.go](./example_test.go).

### SQL Rate Limiter

```go
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
```

### Redis Rate Limiter

```go
limiter, err := redisrl.New(context.Background(), redisCli)
if err != nil {
    panic(err)
}
```

### Using Reserve Method

The `Reserve` method handles both immediate decisions and future reservations based on `MaxFutureReserve`:

**Immediate Decision (MaxFutureReserve = 0):**

```go
r, err := limiter.Reserve(ctx, &ratelimiter.ReserveRequest{
    Key:              "user:123",
    DurationPerToken: 10 * time.Minute,
    Burst:            5,
    Tokens:           1,
    MaxFutureReserve: 0, // No waiting allowed
})

if r.OK {
    fmt.Println("Request allowed, proceed immediately")
    // Perform action right away
} else {
    retryAfter := r.MustRetryAfterFrom(time.Now())
    fmt.Printf("Request denied, retry after %v\n", retryAfter)
}
```

**Future Reservation (MaxFutureReserve > 0):**

```go
r, err := limiter.Reserve(ctx, &ratelimiter.ReserveRequest{
    Key:              "user:123",
    DurationPerToken: 10 * time.Minute,
    Burst:            5,
    Tokens:           1,
    MaxFutureReserve: 30 * time.Second, // Allow up to 30s wait
})

if r.OK {
    delay := r.MustDelayFrom(time.Now())
    if delay > 0 {
        fmt.Printf("Request allowed, wait %v before acting\n", delay)
        time.Sleep(delay) // Wait before proceeding
    } else {
        fmt.Println("Request allowed, act immediately")
    }
    // Now perform the action
} else {
    retryAfter := r.MustRetryAfterFrom(time.Now())
    fmt.Printf("Request denied, retry after %v\n", retryAfter)
}
```

**Key Points:**

- `OK = true` means the request **can be satisfied** (possibly with delay)
- `OK = false` means the request **cannot be satisfied** within `MaxFutureReserve`
- ⚠️ **IMPORTANT**: Only call `MustDelayFrom()` when `OK = true`
- ⚠️ **IMPORTANT**: Only call `MustRetryAfterFrom()` when `OK = false`
- Calling these methods with wrong `OK` status may panic
- ⚠️ **TIME PRECISION**: All time calculations are performed with **microsecond precision**.

### Benchmark

```

goos: darwin
goarch: arm64
pkg: github.com/theplant/ratelimiter
cpu: Apple M3 Pro
BenchmarkRedisRateLimiter_Reserve
BenchmarkRedisRateLimiter_Reserve/Key1_Duration10ms_Burst5
BenchmarkRedisRateLimiter_Reserve/Key1_Duration10ms_Burst5-12         	    4760	    278422 ns/op	     724 B/op	      17 allocs/op
BenchmarkRedisRateLimiter_Reserve/Key2_Duration20ms_Burst10
BenchmarkRedisRateLimiter_Reserve/Key2_Duration20ms_Burst10-12        	    4341	    272833 ns/op	     720 B/op	      17 allocs/op
BenchmarkRedisRateLimiter_Reserve/Key3_Duration50ms_Burst3
BenchmarkRedisRateLimiter_Reserve/Key3_Duration50ms_Burst3-12         	    4215	    262703 ns/op	     720 B/op	      17 allocs/op

BenchmarkSQLRateLimiter_Reserve
BenchmarkSQLRateLimiter_Reserve/Key1_Duration10ms_Burst5
BenchmarkSQLRateLimiter_Reserve/Key1_Duration10ms_Burst5-12         	    1026	   1102261 ns/op	    9622 B/op	     132 allocs/op
BenchmarkSQLRateLimiter_Reserve/Key2_Duration20ms_Burst10
BenchmarkSQLRateLimiter_Reserve/Key2_Duration20ms_Burst10-12        	    1071	   1102991 ns/op	    9605 B/op	     132 allocs/op
BenchmarkSQLRateLimiter_Reserve/Key3_Duration50ms_Burst3
BenchmarkSQLRateLimiter_Reserve/Key3_Duration50ms_Burst3-12         	    1129	   1012237 ns/op	    9794 B/op	     132 allocs/op

```
