package ratelimiter_test

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
	"github.com/theplant/ratelimiter"
	"github.com/theplant/testenv"
	"gorm.io/gorm"
)

var (
	db       *gorm.DB
	redisCli *redis.Client
)

func TestMain(m *testing.M) {
	env, err := testenv.New().DBEnable(true).SetUp()
	if err != nil {
		panic(err)
	}
	defer env.TearDown()

	db = env.DB
	// db.Logger = db.Logger.LogMode(logger.Info)

	if err = db.AutoMigrate(&ratelimiter.KV{}); err != nil {
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

func resetDB() {
	if err := db.Where("1 = 1").Delete(&ratelimiter.KV{}).Error; err != nil {
		panic(err)
	}
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

func testReverse(t *testing.T, limiter *ratelimiter.RateLimiter, key string) {
	durationPerToken := time.Second
	burst := 10

	now := time.Now()
	testCases := []struct {
		name                string
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
				Now:              now,
				Tokens:           5,
				MaxFutureReserve: 0,
			},
			expectedReservation: nil,
			expectedError:       "invalid parameters",
		},
		{
			name: "enough tokens",
			reserveRequest: &ratelimiter.ReserveRequest{
				Key:              key,
				DurationPerToken: durationPerToken,
				Burst:            burst,
				Now:              now,
				Tokens:           5,
				MaxFutureReserve: 0,
			},
			expectedReservation: &ratelimiter.Reservation{
				OK:        true,
				TimeToAct: now.Add(-10 * time.Second).Add(5 * time.Second),
			},
			expectedError: "",
		},
		{
			name: "insufficient tokens",
			reserveRequest: &ratelimiter.ReserveRequest{
				Key:              key,
				DurationPerToken: durationPerToken,
				Burst:            burst,
				Now:              now,
				Tokens:           6, // 6 tokens requested, but only 5 available
				MaxFutureReserve: 0,
			},
			expectedReservation: &ratelimiter.Reservation{
				OK:        false,
				TimeToAct: now.Add(-10 * time.Second).Add(5 * time.Second).Add(6 * time.Second),
			},
			expectedError: "",
		},
		{
			name: "enough tokens after waiting",
			reserveRequest: &ratelimiter.ReserveRequest{
				Key:              key,
				DurationPerToken: durationPerToken,
				Burst:            burst,
				Now:              now.Add(time.Second), // 6 tokens available after 1 second
				Tokens:           6,
				MaxFutureReserve: 0,
			},
			expectedReservation: &ratelimiter.Reservation{
				OK:        true,
				TimeToAct: now.Add(-10 * time.Second).Add(5 * time.Second).Add(6 * time.Second),
			},
			expectedError: "",
		},
		{
			name: "MaxFutureReserve",
			reserveRequest: &ratelimiter.ReserveRequest{
				Key:              key,
				DurationPerToken: durationPerToken,
				Burst:            burst,
				Now:              now.Add(time.Second),
				Tokens:           3,
				MaxFutureReserve: 3 * time.Second, // 3 seconds in the future
			},
			expectedReservation: &ratelimiter.Reservation{
				OK:        true,
				TimeToAct: now.Add(time.Second).Add(3 * time.Second),
			},
			expectedError: "",
		},
		{
			name: "MaxFutureReserve but not enough tokens",
			reserveRequest: &ratelimiter.ReserveRequest{
				Key:              key,
				DurationPerToken: durationPerToken,
				Burst:            burst,
				Now:              now.Add(time.Second),
				Tokens:           3,
				MaxFutureReserve: 5 * time.Second, // should retry after 1 seconds with MaxFutureReserve 5 seconds
			},
			expectedReservation: &ratelimiter.Reservation{
				OK:        false,
				TimeToAct: now.Add(time.Second).Add(3 * time.Second).Add(3 * time.Second),
			},
			expectedError: "",
		},
		{
			name: "retry after 1 second",
			reserveRequest: &ratelimiter.ReserveRequest{
				Key:              key,
				DurationPerToken: durationPerToken,
				Burst:            burst,
				Now:              now.Add(time.Second).Add(time.Second), // retry after 1 second
				Tokens:           3,
				MaxFutureReserve: 5 * time.Second,
			},
			expectedReservation: &ratelimiter.Reservation{
				OK:        true, // should be OK now
				TimeToAct: now.Add(time.Second).Add(3 * time.Second).Add(3 * time.Second),
			},
			expectedError: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
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
				require.Equal(t, tc.expectedReservation.TimeToAct.UTC(), r.TimeToAct.UTC())

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

func testAllow(t *testing.T, limiter *ratelimiter.RateLimiter, key string) {
	durationPerToken := time.Second
	burst := 10

	now := time.Now()
	testCases := []struct {
		name          string
		allowRequest  *ratelimiter.AllowRequest
		expectedOK    bool
		expectedError string
	}{
		{
			name: "invalid parameters",
			allowRequest: &ratelimiter.AllowRequest{
				Key:              key,
				DurationPerToken: durationPerToken,
				Burst:            0,
				Now:              now,
				Tokens:           5,
			},
			expectedOK:    false,
			expectedError: "invalid parameters",
		},
		{
			name: "enough tokens",
			allowRequest: &ratelimiter.AllowRequest{
				Key:              key,
				DurationPerToken: durationPerToken,
				Burst:            burst,
				Now:              now,
				Tokens:           5,
			},
			expectedOK:    true,
			expectedError: "",
		},
		{
			name: "insufficient tokens",
			allowRequest: &ratelimiter.AllowRequest{
				Key:              key,
				DurationPerToken: durationPerToken,
				Burst:            burst,
				Now:              now,
				Tokens:           6, // 6 tokens requested, but only 5 available
			},
			expectedOK:    false,
			expectedError: "",
		},
		{
			name: "enough tokens after waiting",
			allowRequest: &ratelimiter.AllowRequest{
				Key:              key,
				DurationPerToken: durationPerToken,
				Burst:            burst,
				Now:              now.Add(time.Second), // 6 tokens available after 1 second
				Tokens:           6,
			},
			expectedOK:    true,
			expectedError: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ok, err := limiter.Allow(context.Background(), tc.allowRequest)
			if tc.expectedError != "" {
				require.ErrorContains(t, err, tc.expectedError)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.expectedOK, ok)
		})
	}
}

func TestReverse_DriverGORM(t *testing.T) {
	resetDB()
	testReverse(t, ratelimiter.New(
		ratelimiter.DriverGORM(db),
	), "TestReverse_DriverGORM")
}

func TestAllow_DriverGORM(t *testing.T) {
	resetDB()
	testAllow(t, ratelimiter.New(
		ratelimiter.DriverGORM(db),
	), "TestAllow_DriverGORM")
}

func TestReverse_DriverRedis(t *testing.T) {
	resetDB()
	testReverse(t, ratelimiter.New(
		ratelimiter.DriverRedis(redisCli),
	), "TestReverse_DriverRedis")
}

func TestAllow_DriverRedis(t *testing.T) {
	resetDB()
	testAllow(t, ratelimiter.New(
		ratelimiter.DriverRedis(redisCli),
	), "TestAllow_DriverRedis")
}
