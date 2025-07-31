package ratelimiter_test

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"
	"github.com/theplant/ratelimiter"
	"github.com/theplant/ratelimiter/redisrl"
	"github.com/theplant/ratelimiter/sqlrl"
	"github.com/theplant/testenv"
	"gorm.io/gorm"
)

var (
	db       *gorm.DB
	redisCli *redis.Client
)

func TestMain(m *testing.M) {
	var err error
	env, err := testenv.New().DBEnable(true).RedisEnable(true).SetUp()
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := env.TearDown(); err != nil {
			log.Fatalf("Failed to tear down test environment: %v", err)
		}
	}()

	db = env.DB
	redisCli = env.Redis

	// Create SQL rate limiter and migrate table
	sqlLimiter, err := sqlrl.New(db, "kvs")
	if err != nil {
		log.Fatalf("Failed to create SQL rate limiter: %v", err)
	}

	// Use Migrate method to create the table
	ctx := context.Background()
	if err := sqlLimiter.Migrate(ctx); err != nil {
		log.Fatalf("Failed to migrate table: %v", err)
	}

	m.Run()
}

func testReserveWithNowAdvanced(t *testing.T, limiter ratelimiter.RateLimiter, key string) {
	durationPerToken := time.Second
	burst := 10

	now := time.Now()
	testCases := []struct {
		name                string
		reserveRequest      *ratelimiter.ReserveRequest
		now                 time.Time
		expectedReservation *ratelimiter.Reservation
		expectedError       string
	}{
		{
			name: "invalid parameters",
			reserveRequest: &ratelimiter.ReserveRequest{
				Key:              "",
				DurationPerToken: durationPerToken,
				Burst:            burst,
				Tokens:           5,
				MaxFutureReserve: 0,
			},
			now:                 now,
			expectedReservation: nil,
			expectedError:       "key is empty",
		},
		{
			name: "enough tokens",
			reserveRequest: &ratelimiter.ReserveRequest{
				Key:              key,
				DurationPerToken: durationPerToken,
				Burst:            burst,
				Tokens:           5,
				MaxFutureReserve: 0,
			},
			now: now,
			expectedReservation: &ratelimiter.Reservation{
				OK:         true,
				TimeToAct:  now.Add(-10 * durationPerToken).Add(5 * durationPerToken),
				ReservedAt: now,
			},
			expectedError: "",
		},
		{
			name: "insufficient tokens",
			reserveRequest: &ratelimiter.ReserveRequest{
				Key:              key,
				DurationPerToken: durationPerToken,
				Burst:            burst,
				Tokens:           6, // 6 tokens requested, but only 5 available
				MaxFutureReserve: 0,
			},
			now: now,
			expectedReservation: &ratelimiter.Reservation{
				OK:         false,
				TimeToAct:  now.Add(-10 * durationPerToken).Add(5 * durationPerToken).Add(6 * durationPerToken),
				ReservedAt: now,
			},
			expectedError: "",
		},
		{
			name: "enough tokens after waiting",
			reserveRequest: &ratelimiter.ReserveRequest{
				Key:              key,
				DurationPerToken: durationPerToken,
				Burst:            burst,
				Tokens:           6,
				MaxFutureReserve: 0,
			},
			now: now.Add(durationPerToken), // 6 tokens available after 1 second
			expectedReservation: &ratelimiter.Reservation{
				OK:         true,
				TimeToAct:  now.Add(-10 * durationPerToken).Add(5 * durationPerToken).Add(6 * durationPerToken),
				ReservedAt: now.Add(durationPerToken),
			},
			expectedError: "",
		},
		{
			name: "MaxFutureReserve",
			reserveRequest: &ratelimiter.ReserveRequest{
				Key:              key,
				DurationPerToken: durationPerToken,
				Burst:            burst,
				Tokens:           3,
				MaxFutureReserve: 3 * durationPerToken, // 3 seconds in the future
			},
			now: now.Add(durationPerToken),
			expectedReservation: &ratelimiter.Reservation{
				OK:         true,
				TimeToAct:  now.Add(durationPerToken).Add(3 * durationPerToken),
				ReservedAt: now.Add(durationPerToken),
			},
			expectedError: "",
		},
		{
			name: "MaxFutureReserve but not enough tokens",
			reserveRequest: &ratelimiter.ReserveRequest{
				Key:              key,
				DurationPerToken: durationPerToken,
				Burst:            burst,
				Tokens:           3,
				MaxFutureReserve: 5 * durationPerToken, // should retry after 1 seconds with MaxFutureReserve 5 seconds
			},
			now: now.Add(durationPerToken),
			expectedReservation: &ratelimiter.Reservation{
				OK:         false,
				TimeToAct:  now.Add(durationPerToken).Add(3 * durationPerToken).Add(3 * durationPerToken),
				ReservedAt: now.Add(durationPerToken),
			},
			expectedError: "",
		},
		{
			name: "retry after 1 second",
			reserveRequest: &ratelimiter.ReserveRequest{
				Key:              key,
				DurationPerToken: durationPerToken,
				Burst:            burst,
				Tokens:           3,
				MaxFutureReserve: 5 * durationPerToken,
			},
			now: now.Add(durationPerToken).Add(durationPerToken), // retry after 1 second
			expectedReservation: &ratelimiter.Reservation{
				OK:         true, // should be OK now
				TimeToAct:  now.Add(durationPerToken).Add(3 * durationPerToken).Add(3 * durationPerToken),
				ReservedAt: now.Add(durationPerToken).Add(durationPerToken),
			},
			expectedError: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := ratelimiter.WithNowFuncForTest(context.Background(), func() time.Time {
				return tc.now
			})
			r, err := limiter.Reserve(ctx, tc.reserveRequest)
			if tc.expectedError != "" {
				require.ErrorContains(t, err, tc.expectedError)
			} else {
				require.NoError(t, err)
			}

			if tc.expectedReservation == nil {
				require.Nil(t, r)
			} else {
				require.NotNil(t, r)

				require.Equal(t, tc.reserveRequest, r.ReserveRequest)
				require.Equal(t, tc.expectedReservation.OK, r.OK)
				require.Equal(t, tc.expectedReservation.TimeToAct.UTC(), r.TimeToAct.UTC())
				require.Equal(t, tc.expectedReservation.ReservedAt.UTC(), r.ReservedAt.UTC())

				if r.OK {
					_, err := r.RetryAfter()
					require.ErrorContains(t, err, "cannot get retry after from OK reservation")

					delay, err := r.DelayFrom(r.ReservedAt)
					require.NoError(t, err)
					require.GreaterOrEqual(t, delay, time.Duration(0))
					if delay > 0 {
						require.Equal(t, delay, r.TimeToAct.Sub(r.ReservedAt))
					} else {
						require.LessOrEqual(t, r.TimeToAct.Sub(r.ReservedAt), time.Duration(0))
					}
				} else {
					_, err := r.Delay()
					require.ErrorContains(t, err, "cannot get delay from non-OK reservation")

					retryAfter, err := r.RetryAfterFrom(r.ReservedAt)
					require.NoError(t, err)
					require.GreaterOrEqual(t, retryAfter, time.Duration(0))
					if retryAfter > 0 {
						require.Equal(t, retryAfter, r.TimeToAct.Sub(r.ReservedAt)-tc.reserveRequest.MaxFutureReserve)
					} else {
						require.LessOrEqual(t, r.TimeToAct.Sub(r.ReservedAt)-tc.reserveRequest.MaxFutureReserve, time.Duration(0))
					}
				}
			}
		})
	}
}

func testAllowWithNowAdvanced(t *testing.T, limiter ratelimiter.RateLimiter, key string) {
	durationPerToken := time.Second
	burst := 10

	now := time.Now()
	testCases := []struct {
		name          string
		allowRequest  *ratelimiter.AllowRequest
		now           time.Time
		expectedOK    bool
		expectedError string
	}{
		{
			name: "invalid parameters",
			allowRequest: &ratelimiter.AllowRequest{
				Key:              key,
				DurationPerToken: durationPerToken,
				Burst:            0,
				Tokens:           5,
			},
			now:           now,
			expectedOK:    false,
			expectedError: "burst is non-positive",
		},
		{
			name: "enough tokens",
			allowRequest: &ratelimiter.AllowRequest{
				Key:              key,
				DurationPerToken: durationPerToken,
				Burst:            burst,
				Tokens:           5,
			},
			now:           now,
			expectedOK:    true,
			expectedError: "",
		},
		{
			name: "insufficient tokens",
			allowRequest: &ratelimiter.AllowRequest{
				Key:              key,
				DurationPerToken: durationPerToken,
				Burst:            burst,
				Tokens:           6, // 6 tokens requested, but only 5 available
			},
			now:           now,
			expectedOK:    false,
			expectedError: "",
		},
		{
			name: "enough tokens after waiting",
			allowRequest: &ratelimiter.AllowRequest{
				Key:              key,
				DurationPerToken: durationPerToken,
				Burst:            burst,
				Tokens:           6,
			},
			now:           now.Add(durationPerToken), // 6 tokens available after 1 second
			expectedOK:    true,
			expectedError: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := ratelimiter.WithNowFuncForTest(context.Background(), func() time.Time {
				return tc.now
			})
			ok, err := limiter.Allow(ctx, tc.allowRequest)
			if tc.expectedError != "" {
				require.ErrorContains(t, err, tc.expectedError)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.expectedOK, ok)
		})
	}
}

func TestReserveWithNowAdvanced_SQL(t *testing.T) {
	limiter, err := sqlrl.New(db, "kvs")
	require.NoError(t, err)
	testReserveWithNowAdvanced(t, limiter, "TestReserveWithNowAdvanced_SQL")
}

func TestAllowWithNowAdvanced_SQL(t *testing.T) {
	limiter, err := sqlrl.New(db, "kvs")
	require.NoError(t, err)
	testAllowWithNowAdvanced(t, limiter, "TestAllowWithNowAdvanced_SQL")
}

func TestReserveWithNowAdvanced_Redis(t *testing.T) {
	limiter, err := redisrl.New(context.Background(), redisCli)
	require.NoError(t, err)
	testReserveWithNowAdvanced(t, limiter, "TestReserveWithNowAdvanced_Redis")
}

func TestAllowWithNowAdvanced_Redis(t *testing.T) {
	limiter, err := redisrl.New(context.Background(), redisCli)
	require.NoError(t, err)
	testAllowWithNowAdvanced(t, limiter, "TestAllowWithNowAdvanced_Redis")
}

func testReserve(t *testing.T, limiter ratelimiter.RateLimiter, key string) {
	durationPerToken := 100 * time.Millisecond
	burst := 10

	now := time.Now()
	testCases := []struct {
		name                string
		before              func()
		reserveRequest      *ratelimiter.ReserveRequest
		expectedReservation *ratelimiter.Reservation
		expectedError       string
	}{
		{
			name: "invalid parameters",
			reserveRequest: &ratelimiter.ReserveRequest{
				Key:              "",
				DurationPerToken: durationPerToken,
				Burst:            burst,
				Tokens:           5,
				MaxFutureReserve: 0,
			},
			expectedReservation: nil,
			expectedError:       "key is empty",
		},
		{
			name: "enough tokens",
			reserveRequest: &ratelimiter.ReserveRequest{
				Key:              key,
				DurationPerToken: durationPerToken,
				Burst:            burst,
				Tokens:           5,
				MaxFutureReserve: 0,
			},
			expectedReservation: &ratelimiter.Reservation{
				OK:         true,
				TimeToAct:  now.Add(-10 * durationPerToken).Add(5 * durationPerToken),
				ReservedAt: now,
			},
			expectedError: "",
		},
		{
			name: "insufficient tokens",
			reserveRequest: &ratelimiter.ReserveRequest{
				Key:              key,
				DurationPerToken: durationPerToken,
				Burst:            burst,
				Tokens:           6, // 6 tokens requested, but only 5 available
				MaxFutureReserve: 0,
			},
			expectedReservation: &ratelimiter.Reservation{
				OK:         false,
				TimeToAct:  now.Add(-10 * durationPerToken).Add(5 * durationPerToken).Add(6 * durationPerToken),
				ReservedAt: now,
			},
			expectedError: "",
		},
		{
			name: "enough tokens after waiting",
			before: func() {
				time.Sleep(durationPerToken)
			},
			reserveRequest: &ratelimiter.ReserveRequest{
				Key:              key,
				DurationPerToken: durationPerToken,
				Burst:            burst,
				Tokens:           6,
				MaxFutureReserve: 0,
			},
			expectedReservation: &ratelimiter.Reservation{
				OK:         true,
				TimeToAct:  now.Add(-10 * durationPerToken).Add(5 * durationPerToken).Add(6 * durationPerToken),
				ReservedAt: now.Add(durationPerToken),
			},
			expectedError: "",
		},
		{
			name: "MaxFutureReserve",
			reserveRequest: &ratelimiter.ReserveRequest{
				Key:              key,
				DurationPerToken: durationPerToken,
				Burst:            burst,
				Tokens:           3,
				MaxFutureReserve: 3 * durationPerToken, // 3 seconds in the future
			},
			expectedReservation: &ratelimiter.Reservation{
				OK:         true,
				TimeToAct:  now.Add(durationPerToken).Add(3 * durationPerToken),
				ReservedAt: now.Add(durationPerToken),
			},
			expectedError: "",
		},
		{
			name: "MaxFutureReserve but not enough tokens",
			reserveRequest: &ratelimiter.ReserveRequest{
				Key:              key,
				DurationPerToken: durationPerToken,
				Burst:            burst,
				Tokens:           3,
				MaxFutureReserve: 5 * durationPerToken, // should retry after 1 seconds with MaxFutureReserve 5 seconds
			},
			expectedReservation: &ratelimiter.Reservation{
				OK:         false,
				TimeToAct:  now.Add(durationPerToken).Add(3 * durationPerToken).Add(3 * durationPerToken),
				ReservedAt: now.Add(durationPerToken),
			},
			expectedError: "",
		},
		{
			name: "retry after 1 second",
			before: func() {
				time.Sleep(durationPerToken)
			},
			reserveRequest: &ratelimiter.ReserveRequest{
				Key:              key,
				DurationPerToken: durationPerToken,
				Burst:            burst,
				Tokens:           3,
				MaxFutureReserve: 5 * durationPerToken,
			},
			expectedReservation: &ratelimiter.Reservation{
				OK:         true, // should be OK now
				TimeToAct:  now.Add(durationPerToken).Add(3 * durationPerToken).Add(3 * durationPerToken),
				ReservedAt: now.Add(durationPerToken).Add(durationPerToken),
			},
			expectedError: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.before != nil {
				tc.before()
			}
			r, err := limiter.Reserve(context.Background(), tc.reserveRequest)
			if tc.expectedError != "" {
				require.ErrorContains(t, err, tc.expectedError)
			} else {
				require.NoError(t, err)
			}

			if tc.expectedReservation == nil {
				require.Nil(t, r)
			} else {
				require.NotNil(t, r)

				require.Equal(t, tc.reserveRequest, r.ReserveRequest)
				require.Equal(t, tc.expectedReservation.OK, r.OK)
				require.Equal(t, tc.expectedReservation.TimeToAct.Truncate(100*time.Millisecond).UTC(), r.TimeToAct.Truncate(100*time.Millisecond).UTC())
				require.Equal(t, time.Now().Truncate(100*time.Millisecond).UTC(), r.ReservedAt.Truncate(100*time.Millisecond).UTC())
				if r.OK {
					_, err := r.RetryAfter()
					require.ErrorContains(t, err, "cannot get retry after from OK reservation")

					delay, err := r.DelayFrom(r.ReservedAt)
					require.NoError(t, err)
					require.GreaterOrEqual(t, delay, time.Duration(0))
					if delay > 0 {
						require.Equal(t, delay, r.TimeToAct.Sub(r.ReservedAt))
					} else {
						require.LessOrEqual(t, r.TimeToAct.Sub(r.ReservedAt), time.Duration(0))
					}
				} else {
					_, err := r.Delay()
					require.ErrorContains(t, err, "cannot get delay from non-OK reservation")

					retryAfter, err := r.RetryAfterFrom(r.ReservedAt)
					require.NoError(t, err)
					require.GreaterOrEqual(t, retryAfter, time.Duration(0))
					if retryAfter > 0 {
						require.Equal(t, retryAfter, r.TimeToAct.Sub(r.ReservedAt)-tc.reserveRequest.MaxFutureReserve)
					} else {
						require.LessOrEqual(t, r.TimeToAct.Sub(r.ReservedAt)-tc.reserveRequest.MaxFutureReserve, time.Duration(0))
					}
				}
			}
		})
	}
}

func TestReserve_SQL(t *testing.T) {
	limiter, err := sqlrl.New(db, "kvs")
	require.NoError(t, err)
	testReserve(t, limiter, "TestReserve_SQL")
}

func TestReserve_Redis(t *testing.T) {
	limiter, err := redisrl.New(context.Background(), redisCli)
	require.NoError(t, err)
	testReserve(t, limiter, "TestReserve_Redis")
}
