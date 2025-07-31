package ratelimiter_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/theplant/ratelimiter"
)

func TestReserveRequest_Validate(t *testing.T) {
	validReq := &ratelimiter.ReserveRequest{
		Key:              "test-key",
		DurationPerToken: time.Second,
		Burst:            10,
		Tokens:           5,
		MaxFutureReserve: 30 * time.Second,
	}

	testCases := []struct {
		name        string
		modifyReq   func(*ratelimiter.ReserveRequest)
		expectedErr string
	}{
		{
			name: "valid request",
			modifyReq: func(req *ratelimiter.ReserveRequest) {
				// no changes
			},
			expectedErr: "",
		},
		{
			name: "empty key",
			modifyReq: func(req *ratelimiter.ReserveRequest) {
				req.Key = ""
			},
			expectedErr: "key is empty",
		},
		{
			name: "zero duration per token",
			modifyReq: func(req *ratelimiter.ReserveRequest) {
				req.DurationPerToken = 0
			},
			expectedErr: "duration per token is non-positive",
		},
		{
			name: "negative duration per token",
			modifyReq: func(req *ratelimiter.ReserveRequest) {
				req.DurationPerToken = -time.Second
			},
			expectedErr: "duration per token is non-positive",
		},
		{
			name: "zero burst",
			modifyReq: func(req *ratelimiter.ReserveRequest) {
				req.Burst = 0
			},
			expectedErr: "burst is non-positive",
		},
		{
			name: "negative burst",
			modifyReq: func(req *ratelimiter.ReserveRequest) {
				req.Burst = -1
			},
			expectedErr: "burst is non-positive",
		},
		{
			name: "zero tokens",
			modifyReq: func(req *ratelimiter.ReserveRequest) {
				req.Tokens = 0
			},
			expectedErr: "tokens is non-positive",
		},
		{
			name: "negative tokens",
			modifyReq: func(req *ratelimiter.ReserveRequest) {
				req.Tokens = -1
			},
			expectedErr: "tokens is non-positive",
		},
		{
			name: "tokens greater than burst",
			modifyReq: func(req *ratelimiter.ReserveRequest) {
				req.Tokens = 15
				req.Burst = 10
			},
			expectedErr: "tokens is greater than burst",
		},
		{
			name: "negative max future reserve",
			modifyReq: func(req *ratelimiter.ReserveRequest) {
				req.MaxFutureReserve = -time.Second
			},
			expectedErr: "max future reserve is negative",
		},
		{
			name: "zero max future reserve (valid)",
			modifyReq: func(req *ratelimiter.ReserveRequest) {
				req.MaxFutureReserve = 0
			},
			expectedErr: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Copy the valid request
			req := *validReq
			tc.modifyReq(&req)

			err := req.Validate()
			if tc.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedErr)
			}
		})
	}
}

func TestReservation_MustMethods(t *testing.T) {
	now := time.Now()

	t.Run("MustDelay on OK reservation", func(t *testing.T) {
		reservation := &ratelimiter.Reservation{
			ReserveRequest: &ratelimiter.ReserveRequest{
				Key:              "test",
				DurationPerToken: time.Second,
				Burst:            10,
				Tokens:           1,
				MaxFutureReserve: 10 * time.Second,
			},
			OK:         true,
			TimeToAct:  now.Add(5 * time.Second),
			ReservedAt: now,
		}

		delay := reservation.MustDelay()
		// Allow for small timing differences
		require.InDelta(t, float64(5*time.Second), float64(delay), float64(100*time.Millisecond))
	})

	t.Run("MustDelay on non-OK reservation should panic", func(t *testing.T) {
		reservation := &ratelimiter.Reservation{
			ReserveRequest: &ratelimiter.ReserveRequest{
				Key:              "test",
				DurationPerToken: time.Second,
				Burst:            10,
				Tokens:           1,
				MaxFutureReserve: 10 * time.Second,
			},
			OK:         false,
			TimeToAct:  now.Add(5 * time.Second),
			ReservedAt: now,
		}

		require.Panics(t, func() {
			reservation.MustDelay()
		})
	})

	t.Run("MustDelayFrom on OK reservation", func(t *testing.T) {
		timeToAct := now.Add(5 * time.Second)
		reservation := &ratelimiter.Reservation{
			ReserveRequest: &ratelimiter.ReserveRequest{
				Key:              "test",
				DurationPerToken: time.Second,
				Burst:            10,
				Tokens:           1,
				MaxFutureReserve: 10 * time.Second,
			},
			OK:         true,
			TimeToAct:  timeToAct,
			ReservedAt: now,
		}

		customTime := now.Add(2 * time.Second)
		delay := reservation.MustDelayFrom(customTime)
		expectedDelay := timeToAct.Sub(customTime)
		require.Equal(t, expectedDelay, delay)
	})

	t.Run("MustDelayFrom on non-OK reservation should panic", func(t *testing.T) {
		reservation := &ratelimiter.Reservation{
			ReserveRequest: &ratelimiter.ReserveRequest{
				Key:              "test",
				DurationPerToken: time.Second,
				Burst:            10,
				Tokens:           1,
				MaxFutureReserve: 10 * time.Second,
			},
			OK:         false,
			TimeToAct:  now.Add(5 * time.Second),
			ReservedAt: now,
		}

		require.Panics(t, func() {
			reservation.MustDelayFrom(now)
		})
	})

	t.Run("MustRetryAfter on non-OK reservation", func(t *testing.T) {
		timeToAct := now.Add(15 * time.Second)
		maxFutureReserve := 10 * time.Second
		reservation := &ratelimiter.Reservation{
			ReserveRequest: &ratelimiter.ReserveRequest{
				Key:              "test",
				DurationPerToken: time.Second,
				Burst:            10,
				Tokens:           1,
				MaxFutureReserve: maxFutureReserve,
			},
			OK:         false,
			TimeToAct:  timeToAct, // Beyond MaxFutureReserve
			ReservedAt: now,
		}

		retryAfter := reservation.MustRetryAfter()
		// RetryAfter = TimeToAct.Sub(now) - MaxFutureReserve = 15s - 10s = 5s
		expectedRetryAfter := timeToAct.Sub(now) - maxFutureReserve
		// Allow for small timing differences
		require.InDelta(t, float64(expectedRetryAfter), float64(retryAfter), float64(100*time.Millisecond))
	})

	t.Run("MustRetryAfter on OK reservation should panic", func(t *testing.T) {
		reservation := &ratelimiter.Reservation{
			ReserveRequest: &ratelimiter.ReserveRequest{
				Key:              "test",
				DurationPerToken: time.Second,
				Burst:            10,
				Tokens:           1,
				MaxFutureReserve: 10 * time.Second,
			},
			OK:         true,
			TimeToAct:  now.Add(5 * time.Second),
			ReservedAt: now,
		}

		require.Panics(t, func() {
			reservation.MustRetryAfter()
		})
	})

	t.Run("MustRetryAfterFrom on non-OK reservation", func(t *testing.T) {
		timeToAct := now.Add(15 * time.Second)
		maxFutureReserve := 10 * time.Second
		reservation := &ratelimiter.Reservation{
			ReserveRequest: &ratelimiter.ReserveRequest{
				Key:              "test",
				DurationPerToken: time.Second,
				Burst:            10,
				Tokens:           1,
				MaxFutureReserve: maxFutureReserve,
			},
			OK:         false,
			TimeToAct:  timeToAct, // Beyond MaxFutureReserve
			ReservedAt: now,
		}

		customTime := now.Add(3 * time.Second)
		retryAfter := reservation.MustRetryAfterFrom(customTime)
		// RetryAfter = TimeToAct.Sub(customTime) - MaxFutureReserve = 12s - 10s = 2s
		expectedRetryAfter := timeToAct.Sub(customTime) - maxFutureReserve
		require.Equal(t, expectedRetryAfter, retryAfter)
	})

	t.Run("MustRetryAfterFrom on OK reservation should panic", func(t *testing.T) {
		reservation := &ratelimiter.Reservation{
			ReserveRequest: &ratelimiter.ReserveRequest{
				Key:              "test",
				DurationPerToken: time.Second,
				Burst:            10,
				Tokens:           1,
				MaxFutureReserve: 10 * time.Second,
			},
			OK:         true,
			TimeToAct:  now.Add(5 * time.Second),
			ReservedAt: now,
		}

		require.Panics(t, func() {
			reservation.MustRetryAfterFrom(now)
		})
	})
}

func TestReservation_DelayFromEdgeCases(t *testing.T) {
	now := time.Now()

	t.Run("DelayFrom with past TimeToAct", func(t *testing.T) {
		reservation := &ratelimiter.Reservation{
			ReserveRequest: &ratelimiter.ReserveRequest{
				Key:              "test",
				DurationPerToken: time.Second,
				Burst:            10,
				Tokens:           1,
				MaxFutureReserve: 10 * time.Second,
			},
			OK:         true,
			TimeToAct:  now.Add(-5 * time.Second), // In the past
			ReservedAt: now,
		}

		delay, err := reservation.DelayFrom(now)
		require.NoError(t, err)
		require.Equal(t, time.Duration(0), delay)
	})
}

func TestReservation_RetryAfterFromEdgeCases(t *testing.T) {
	now := time.Now()

	t.Run("RetryAfterFrom with past TimeToAct", func(t *testing.T) {
		reservation := &ratelimiter.Reservation{
			ReserveRequest: &ratelimiter.ReserveRequest{
				Key:              "test",
				DurationPerToken: time.Second,
				Burst:            10,
				Tokens:           1,
				MaxFutureReserve: 10 * time.Second,
			},
			OK:         false,
			TimeToAct:  now.Add(-5 * time.Second), // In the past
			ReservedAt: now,
		}

		retryAfter, err := reservation.RetryAfterFrom(now)
		require.NoError(t, err)
		require.Equal(t, time.Duration(0), retryAfter)
	})
}
