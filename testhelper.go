package ratelimiter

import (
	"context"
	"time"
)

// ctxKeyTestMode is used to enable test mode via context
type ctxKeyTestMode struct{}

// ctxKeyNowFuncForTest is used to inject a custom time function for testing
type ctxKeyNowFuncForTest struct{}

// WithTestMode returns a context with test mode enabled.
// This replaces the global Test variable for better isolation.
func WithTestMode(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxKeyTestMode{}, true)
}

// IsTestMode checks if test mode is enabled in the context.
func IsTestMode(ctx context.Context) bool {
	testMode, ok := ctx.Value(ctxKeyTestMode{}).(bool)
	return ok && testMode
}

// WithNowFuncForTest returns a context with a custom time function for testing.
// This is kept public as it may be used by external tests that use this library.
func WithNowFuncForTest(ctx context.Context, nowFunc func() time.Time) context.Context {
	ctx = WithTestMode(ctx) // automatically enable test mode
	return context.WithValue(ctx, ctxKeyNowFuncForTest{}, nowFunc)
}

// NowFuncFromContextForTest extracts the custom time function from context.
func NowFuncFromContextForTest(ctx context.Context) (func() time.Time, bool) {
	nowFunc, ok := ctx.Value(ctxKeyNowFuncForTest{}).(func() time.Time)
	return nowFunc, ok
}
