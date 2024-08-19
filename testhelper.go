package ratelimiter

import (
	"context"
	"time"
)

var Test = false

type ctxKeyNowFuncForTest struct{}

func WithNowFuncForTest(ctx context.Context, nowFunc func() time.Time) context.Context {
	return context.WithValue(ctx, ctxKeyNowFuncForTest{}, nowFunc)
}

func NowFuncFromContextForTest(ctx context.Context) (func() time.Time, bool) {
	nowFunc, ok := ctx.Value(ctxKeyNowFuncForTest{}).(func() time.Time)
	return nowFunc, ok
}
