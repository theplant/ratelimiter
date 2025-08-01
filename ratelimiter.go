package ratelimiter

import (
	"context"
	"time"

	"github.com/pkg/errors"
)

// RateLimiter provides rate limiting functionality.
// It implements the token bucket algorithm where tokens are replenished at a steady rate
// and consumers can burst up to the bucket capacity.
type RateLimiter interface {
	// Reserve attempts to reserve the specified number of tokens.
	// If successful (OK=true), the caller should wait until TimeToAct before proceeding.
	// If unsuccessful (OK=false), the caller should wait until RetryAfter before trying again.
	Reserve(ctx context.Context, req *ReserveRequest) (*Reservation, error)
}

// ReserveRequest represents a request to reserve tokens, potentially in the future.
// Set MaxFutureReserve to 0 for immediate allow/deny decisions.
type ReserveRequest struct {
	// Key is the unique identifier for the rate limiter bucket
	Key string
	// DurationPerToken defines how long each token takes to regenerate
	DurationPerToken time.Duration
	// Burst is the maximum number of tokens that can be consumed at once
	Burst int
	// Tokens is the number of tokens requested
	Tokens int
	// MaxFutureReserve is the maximum duration into the future that tokens can be reserved
	MaxFutureReserve time.Duration
}

// Validate validates the ReserveRequest parameters.
func (req *ReserveRequest) Validate() error {
	if req == nil {
		return errors.Wrap(ErrInvalidReserveRequest, "request is nil")
	}
	if req.Key == "" {
		return errors.Wrap(ErrInvalidReserveRequest, "key is empty")
	}
	if req.DurationPerToken <= 0 {
		return errors.Wrap(ErrInvalidReserveRequest, "duration per token is non-positive")
	}
	if req.Burst <= 0 {
		return errors.Wrap(ErrInvalidReserveRequest, "burst is non-positive")
	}
	if req.Tokens <= 0 {
		return errors.Wrap(ErrInvalidReserveRequest, "tokens is non-positive")
	}
	if req.Tokens > req.Burst {
		return errors.Wrap(ErrInvalidReserveRequest, "tokens is greater than burst")
	}
	if req.MaxFutureReserve < 0 {
		return errors.Wrap(ErrInvalidReserveRequest, "max future reserve is negative")
	}
	return nil
}

// Reservation represents a reservation for tokens from a rate limiter.
type Reservation struct {
	*ReserveRequest
	// OK indicates whether the reservation was successful
	OK bool
	// TimeToAct is the time when the action should be performed
	TimeToAct time.Time
	// ReservedAt is the time when the reservation was made
	ReservedAt time.Time
}

// DelayFrom returns the duration to wait before acting from the given time.
// Returns an error if called on a non-OK reservation.
func (r *Reservation) DelayFrom(t time.Time) (time.Duration, error) {
	if !r.OK {
		return 0, errors.New("cannot get delay from non-OK reservation")
	}

	delay := r.TimeToAct.Sub(t)
	if delay < 0 {
		return 0, nil
	}
	return delay, nil
}

// Delay returns the duration to wait before acting from now.
// Returns an error if called on a non-OK reservation.
func (r *Reservation) Delay() (time.Duration, error) {
	return r.DelayFrom(time.Now())
}

// RetryAfterFrom returns the duration to wait before retrying from the given time.
// Returns an error if called on an OK reservation.
func (r *Reservation) RetryAfterFrom(t time.Time) (time.Duration, error) {
	if r.OK {
		return 0, errors.New("cannot get retry after from OK reservation")
	}

	// We subtract MaxFutureReserve to calculate the minimum wait time before retrying
	// with the same ReserveRequest can succeed.
	retryAfter := r.TimeToAct.Sub(t) - r.MaxFutureReserve
	if retryAfter < 0 {
		return 0, nil
	}
	return retryAfter, nil
}

// RetryAfter returns the duration to wait before retrying from now.
// Returns an error if called on an OK reservation.
func (r *Reservation) RetryAfter() (time.Duration, error) {
	return r.RetryAfterFrom(time.Now())
}

// MustDelay returns the duration to wait before acting from now.
// Panics if called on a non-OK reservation.
func (r *Reservation) MustDelay() time.Duration {
	delay, err := r.Delay()
	if err != nil {
		panic(err)
	}
	return delay
}

// MustDelayFrom returns the duration to wait before acting from the given time.
// Panics if called on a non-OK reservation.
func (r *Reservation) MustDelayFrom(t time.Time) time.Duration {
	delay, err := r.DelayFrom(t)
	if err != nil {
		panic(err)
	}
	return delay
}

// MustRetryAfter returns the duration to wait before retrying from now.
// Panics if called on an OK reservation.
func (r *Reservation) MustRetryAfter() time.Duration {
	retryAfter, err := r.RetryAfter()
	if err != nil {
		panic(err)
	}
	return retryAfter
}

// MustRetryAfterFrom returns the duration to wait before retrying from the given time.
// Panics if called on an OK reservation.
func (r *Reservation) MustRetryAfterFrom(t time.Time) time.Duration {
	retryAfter, err := r.RetryAfterFrom(t)
	if err != nil {
		panic(err)
	}
	return retryAfter
}
