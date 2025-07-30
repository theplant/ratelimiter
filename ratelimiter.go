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

	// Allow checks if the specified number of tokens are available immediately.
	// It returns true if the request can be satisfied without waiting, false otherwise.
	Allow(ctx context.Context, req *AllowRequest) (bool, error)
}

// ReserveRequest represents a request to reserve tokens, potentially in the future.
// This is the core request type - AllowRequest is implemented as a Reserve operation
// with MaxFutureReserve set to 0.
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

	delay := r.TimeToAct.Sub(t) - r.MaxFutureReserve
	if delay < 0 {
		return 0, nil
	}
	return delay, nil
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

// AllowRequest represents a request to check if tokens are available immediately.
// It contains all the parameters needed to perform a rate limiting check.
type AllowRequest struct {
	// Key is the unique identifier for the rate limiter bucket
	Key string
	// DurationPerToken defines how long each token takes to regenerate
	DurationPerToken time.Duration
	// Burst is the maximum number of tokens that can be consumed at once
	Burst int
	// Tokens is the number of tokens requested
	Tokens int
}

// Allow is a common implementation for the Allow method that can be used by all RateLimiter implementations.
// It converts an AllowRequest to a ReserveRequest with MaxFutureReserve set to 0.
func Allow(ctx context.Context, limiter RateLimiter, req *AllowRequest) (bool, error) {
	reserveReq := &ReserveRequest{
		Key:              req.Key,
		DurationPerToken: req.DurationPerToken,
		Burst:            req.Burst,
		Tokens:           req.Tokens,
		MaxFutureReserve: 0,
	}

	reservation, err := limiter.Reserve(ctx, reserveReq)
	if err != nil {
		return false, err
	}
	return reservation.OK, nil
}
