package ratelimiter

import (
	"cmp"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"
	testredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/theplant/testenv"
	"gorm.io/gorm"
)

var (
	db       *gorm.DB
	redisCli *redis.Client
)

func TestMain(m *testing.M) {
	Test = true
	defer func() {
		Test = false
	}()

	env, err := testenv.New().DBEnable(true).SetUp()
	if err != nil {
		panic(err)
	}
	defer env.TearDown()

	db = env.DB
	// db.Logger = db.Logger.LogMode(logger.Info)

	if err = db.AutoMigrate(&KV{}); err != nil {
		panic(err)
	}

	var cleanupRedis func() error
	redisCli, cleanupRedis, err = setupRedis(context.Background())
	if err != nil {
		panic(err)
	}
	defer cleanupRedis()

	m.Run()
}

func setupRedis(ctx context.Context) (_ *redis.Client, _ func() error, xerr error) {
	container, err := testredis.Run(ctx,
		"redis:7.4.0-alpine",
	)
	if err != nil {
		return nil, nil, fmt.Errorf("fail to start container: %w", err)
	}
	defer func() {
		if xerr != nil {
			container.Terminate(context.Background())
		}
	}()

	endpoint, err := container.ConnectionString(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("fail to get endpoint: %w", err)
	}

	client := redis.NewClient(&redis.Options{
		Addr: strings.TrimPrefix(endpoint, "redis://"),
	})

	return client, func() error {
		return cmp.Or(
			client.Close(),
			container.Terminate(context.Background()),
		)
	}, nil
}

func testReverseWithNowAdvanced(t *testing.T, limiter *RateLimiter, key string) {
	durationPerToken := time.Second
	burst := 10

	now := time.Now()
	testCases := []struct {
		name                string
		reserveRequest      *ReserveRequest
		now                 time.Time
		expectedReservation *Reservation
		expectedError       string
	}{
		{
			name: "invalid parameters",
			reserveRequest: &ReserveRequest{
				Key:              "",
				DurationPerToken: durationPerToken,
				Burst:            burst,
				Tokens:           5,
				MaxFutureReserve: 0,
			},
			now:                 now,
			expectedReservation: nil,
			expectedError:       "invalid parameters",
		},
		{
			name: "enough tokens",
			reserveRequest: &ReserveRequest{
				Key:              key,
				DurationPerToken: durationPerToken,
				Burst:            burst,
				Tokens:           5,
				MaxFutureReserve: 0,
			},
			now: now,
			expectedReservation: &Reservation{
				OK:        true,
				TimeToAct: now.Add(-10 * durationPerToken).Add(5 * durationPerToken),
				Now:       now,
			},
			expectedError: "",
		},
		{
			name: "insufficient tokens",
			reserveRequest: &ReserveRequest{
				Key:              key,
				DurationPerToken: durationPerToken,
				Burst:            burst,
				Tokens:           6, // 6 tokens requested, but only 5 available
				MaxFutureReserve: 0,
			},
			now: now,
			expectedReservation: &Reservation{
				OK:        false,
				TimeToAct: now.Add(-10 * durationPerToken).Add(5 * durationPerToken).Add(6 * durationPerToken),
				Now:       now,
			},
			expectedError: "",
		},
		{
			name: "enough tokens after waiting",
			reserveRequest: &ReserveRequest{
				Key:              key,
				DurationPerToken: durationPerToken,
				Burst:            burst,
				Tokens:           6,
				MaxFutureReserve: 0,
			},
			now: now.Add(durationPerToken), // 6 tokens available after 1 second
			expectedReservation: &Reservation{
				OK:        true,
				TimeToAct: now.Add(-10 * durationPerToken).Add(5 * durationPerToken).Add(6 * durationPerToken),
				Now:       now.Add(durationPerToken),
			},
			expectedError: "",
		},
		{
			name: "MaxFutureReserve",
			reserveRequest: &ReserveRequest{
				Key:              key,
				DurationPerToken: durationPerToken,
				Burst:            burst,
				Tokens:           3,
				MaxFutureReserve: 3 * durationPerToken, // 3 seconds in the future
			},
			now: now.Add(durationPerToken),
			expectedReservation: &Reservation{
				OK:        true,
				TimeToAct: now.Add(durationPerToken).Add(3 * durationPerToken),
				Now:       now.Add(durationPerToken),
			},
			expectedError: "",
		},
		{
			name: "MaxFutureReserve but not enough tokens",
			reserveRequest: &ReserveRequest{
				Key:              key,
				DurationPerToken: durationPerToken,
				Burst:            burst,
				Tokens:           3,
				MaxFutureReserve: 5 * durationPerToken, // should retry after 1 seconds with MaxFutureReserve 5 seconds
			},
			now: now.Add(durationPerToken),
			expectedReservation: &Reservation{
				OK:        false,
				TimeToAct: now.Add(durationPerToken).Add(3 * durationPerToken).Add(3 * durationPerToken),
				Now:       now.Add(durationPerToken),
			},
			expectedError: "",
		},
		{
			name: "retry after 1 second",
			reserveRequest: &ReserveRequest{
				Key:              key,
				DurationPerToken: durationPerToken,
				Burst:            burst,
				Tokens:           3,
				MaxFutureReserve: 5 * durationPerToken,
			},
			now: now.Add(durationPerToken).Add(durationPerToken), // retry after 1 second
			expectedReservation: &Reservation{
				OK:        true, // should be OK now
				TimeToAct: now.Add(durationPerToken).Add(3 * durationPerToken).Add(3 * durationPerToken),
				Now:       now.Add(durationPerToken).Add(durationPerToken),
			},
			expectedError: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := WithNowFuncForTest(context.Background(), func() time.Time {
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
				require.Equal(t, tc.expectedReservation.Now.UTC(), r.Now.UTC())

				if r.OK {
					require.PanicsWithValue(t, "ratelimiter: cannot get retry after from OK reservation", func() {
						_ = r.RetryAfter()
					})
					delay := r.DelayFrom(r.Now)
					require.GreaterOrEqual(t, delay, time.Duration(0))
					if delay > 0 {
						require.Equal(t, delay, r.TimeToAct.Sub(r.Now))
					} else {
						require.LessOrEqual(t, r.TimeToAct.Sub(r.Now), time.Duration(0))
					}
				} else {
					require.PanicsWithValue(t, "ratelimiter: cannot get delay from non-OK reservation", func() {
						_ = r.Delay()
					})
					retryAfter := r.RetryAfterFrom(r.Now)
					require.GreaterOrEqual(t, retryAfter, time.Duration(0))
					if retryAfter > 0 {
						require.Equal(t, retryAfter, r.TimeToAct.Sub(r.Now)-tc.reserveRequest.MaxFutureReserve)
					} else {
						require.LessOrEqual(t, r.TimeToAct.Sub(r.Now)-tc.reserveRequest.MaxFutureReserve, time.Duration(0))
					}
				}
			}
		})
	}
}

func testAllowWithNowAdvanced(t *testing.T, limiter *RateLimiter, key string) {
	durationPerToken := time.Second
	burst := 10

	now := time.Now()
	testCases := []struct {
		name          string
		allowRequest  *AllowRequest
		now           time.Time
		expectedOK    bool
		expectedError string
	}{
		{
			name: "invalid parameters",
			allowRequest: &AllowRequest{
				Key:              key,
				DurationPerToken: durationPerToken,
				Burst:            0,
				Tokens:           5,
			},
			now:           now,
			expectedOK:    false,
			expectedError: "invalid parameters",
		},
		{
			name: "enough tokens",
			allowRequest: &AllowRequest{
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
			allowRequest: &AllowRequest{
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
			allowRequest: &AllowRequest{
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
			ctx := WithNowFuncForTest(context.Background(), func() time.Time {
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

func TestReverseWithNowAdvanced_DriverGORM(t *testing.T) {
	testReverseWithNowAdvanced(t, New(
		NewGormDriver(db),
	), "TestReverseWithNowAdvanced_DriverGORM")
}

func TestAllowWithNowAdvanced_DriverGORM(t *testing.T) {
	testAllowWithNowAdvanced(t, New(
		NewGormDriver(db),
	), "TestAllowWithNowAdvanced_DriverGORM")
}

func TestReverseWithNowAdvanced_DriverRedis(t *testing.T) {
	d, err := InitRedisDriver(context.Background(), redisCli)
	if err != nil {
		panic(err)
	}
	testReverseWithNowAdvanced(t, New(d), "TestReverseWithNowAdvanced_DriverRedis")
}

func TestAllowWithNowAdvanced_DriverRedis(t *testing.T) {
	d, err := InitRedisDriver(context.Background(), redisCli)
	if err != nil {
		panic(err)
	}
	testAllowWithNowAdvanced(t, New(d), "TestAllowWithNowAdvanced_DriverRedis")
}

func testReverse(t *testing.T, limiter *RateLimiter, key string) {
	durationPerToken := 100 * time.Millisecond
	burst := 10

	now := time.Now()
	testCases := []struct {
		name                string
		before              func()
		reserveRequest      *ReserveRequest
		expectedReservation *Reservation
		expectedError       string
	}{
		{
			name: "invalid parameters",
			reserveRequest: &ReserveRequest{
				Key:              "",
				DurationPerToken: durationPerToken,
				Burst:            burst,
				Tokens:           5,
				MaxFutureReserve: 0,
			},
			expectedReservation: nil,
			expectedError:       "invalid parameters",
		},
		{
			name: "enough tokens",
			reserveRequest: &ReserveRequest{
				Key:              key,
				DurationPerToken: durationPerToken,
				Burst:            burst,
				Tokens:           5,
				MaxFutureReserve: 0,
			},
			expectedReservation: &Reservation{
				OK:        true,
				TimeToAct: now.Add(-10 * durationPerToken).Add(5 * durationPerToken),
			},
			expectedError: "",
		},
		{
			name: "insufficient tokens",
			reserveRequest: &ReserveRequest{
				Key:              key,
				DurationPerToken: durationPerToken,
				Burst:            burst,
				Tokens:           6, // 6 tokens requested, but only 5 available
				MaxFutureReserve: 0,
			},
			expectedReservation: &Reservation{
				OK:        false,
				TimeToAct: now.Add(-10 * durationPerToken).Add(5 * durationPerToken).Add(6 * durationPerToken),
			},
			expectedError: "",
		},
		{
			name: "enough tokens after waiting",
			before: func() {
				time.Sleep(durationPerToken)
			},
			reserveRequest: &ReserveRequest{
				Key:              key,
				DurationPerToken: durationPerToken,
				Burst:            burst,
				Tokens:           6,
				MaxFutureReserve: 0,
			},
			expectedReservation: &Reservation{
				OK:        true,
				TimeToAct: now.Add(-10 * durationPerToken).Add(5 * durationPerToken).Add(6 * durationPerToken),
			},
			expectedError: "",
		},
		{
			name: "MaxFutureReserve",
			reserveRequest: &ReserveRequest{
				Key:              key,
				DurationPerToken: durationPerToken,
				Burst:            burst,
				Tokens:           3,
				MaxFutureReserve: 3 * durationPerToken, // 3 seconds in the future
			},
			expectedReservation: &Reservation{
				OK:        true,
				TimeToAct: now.Add(durationPerToken).Add(3 * durationPerToken),
			},
			expectedError: "",
		},
		{
			name: "MaxFutureReserve but not enough tokens",
			reserveRequest: &ReserveRequest{
				Key:              key,
				DurationPerToken: durationPerToken,
				Burst:            burst,
				Tokens:           3,
				MaxFutureReserve: 5 * durationPerToken, // should retry after 1 seconds with MaxFutureReserve 5 seconds
			},
			expectedReservation: &Reservation{
				OK:        false,
				TimeToAct: now.Add(durationPerToken).Add(3 * durationPerToken).Add(3 * durationPerToken),
			},
			expectedError: "",
		},
		{
			name: "retry after 1 second",
			before: func() {
				time.Sleep(durationPerToken)
			},
			reserveRequest: &ReserveRequest{
				Key:              key,
				DurationPerToken: durationPerToken,
				Burst:            burst,
				Tokens:           3,
				MaxFutureReserve: 5 * durationPerToken,
			},
			expectedReservation: &Reservation{
				OK:        true, // should be OK now
				TimeToAct: now.Add(durationPerToken).Add(3 * durationPerToken).Add(3 * durationPerToken),
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
				require.Equal(t, time.Now().Truncate(100*time.Millisecond).UTC(), r.Now.Truncate(100*time.Millisecond).UTC())
				if r.OK {
					require.PanicsWithValue(t, "ratelimiter: cannot get retry after from OK reservation", func() {
						_ = r.RetryAfter()
					})
					delay := r.DelayFrom(r.Now)
					require.GreaterOrEqual(t, delay, time.Duration(0))
					if delay > 0 {
						require.Equal(t, delay, r.TimeToAct.Sub(r.Now))
					} else {
						require.LessOrEqual(t, r.TimeToAct.Sub(r.Now), time.Duration(0))
					}
				} else {
					require.PanicsWithValue(t, "ratelimiter: cannot get delay from non-OK reservation", func() {
						_ = r.Delay()
					})
					retryAfter := r.RetryAfterFrom(r.Now)
					require.GreaterOrEqual(t, retryAfter, time.Duration(0))
					if retryAfter > 0 {
						require.Equal(t, retryAfter, r.TimeToAct.Sub(r.Now)-tc.reserveRequest.MaxFutureReserve)
					} else {
						require.LessOrEqual(t, r.TimeToAct.Sub(r.Now)-tc.reserveRequest.MaxFutureReserve, time.Duration(0))
					}
				}
			}
		})
	}
}

func TestReverse_DriverGORM(t *testing.T) {
	testReverse(t, New(
		NewGormDriver(db),
	), "TestReverse_DriverGORM")
}

func TestReverse_DriverRedis(t *testing.T) {
	d, err := InitRedisDriver(context.Background(), redisCli)
	if err != nil {
		panic(err)
	}
	testReverse(t, New(d), "TestReverse_DriverRedis")
}
