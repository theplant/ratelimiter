package ratelimiter

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestGormForUpdate(t *testing.T) {
	gormDebug = true
	defer func() {
		gormDebug = false
	}()

	ctx := context.Background()
	driver, err := InitGormDriver(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	limiter := New(driver)

	key := "TestGormForUpdate"
	durationPerToken := 100 * time.Millisecond
	burst := 10

	{
		r, err := limiter.Reserve(ctx, &ReserveRequest{
			Key:              key,
			DurationPerToken: durationPerToken,
			Burst:            burst,
			Now:              time.Time{},
			Tokens:           1,
			MaxFutureReserve: 0,
		})
		require.NoError(t, err)
		require.True(t, r.OK)
	}

	sig := make(chan struct{})
	var a, b, c, d, e time.Time
	var nowB, nowE time.Time

	var errG errgroup.Group
	errG.Go(func() error {
		a = time.Now()
		ctx := context.WithValue(ctx, ctxKeyAfterQuery{}, func(kv kvWrapper) {
			nowB = kv.Now
			b = time.Now()
			// make another goroutine to continue after for update
			close(sig)
			// test whether the second goroutine can continue before the sleep done
			time.Sleep(time.Second)
			d = time.Now()
		})
		r, err := limiter.Reserve(ctx, &ReserveRequest{
			Key:              key,
			DurationPerToken: durationPerToken,
			Burst:            burst,
			Now:              time.Time{},
			Tokens:           5,
			MaxFutureReserve: 0,
		})
		if err != nil {
			return err
		}
		require.True(t, r.OK)
		return nil
	})
	errG.Go(func() error {
		<-sig
		c = time.Now()
		ctx := context.WithValue(ctx, ctxKeyAfterQuery{}, func(kv kvWrapper) {
			nowE = kv.Now // need to ensure now is the time after blocking
			e = time.Now()
		})
		r, err := limiter.Reserve(ctx, &ReserveRequest{
			Key:              key,
			DurationPerToken: durationPerToken,
			Burst:            burst,
			Now:              time.Time{},
			Tokens:           3,
			MaxFutureReserve: 0,
		})
		if err != nil {
			return err
		}
		require.True(t, r.OK)
		return nil
	})
	if err := errG.Wait(); err != nil {
		t.Fatal(err)
	}
	t.Logf("a: %v", a)
	t.Logf("b: %v", b)
	t.Logf("nowB: %v", nowB)
	t.Logf("c: %v", c)
	t.Logf("d: %v", d)
	t.Logf("e: %v", e)
	t.Logf("nowE: %v", nowE)

	require.Truef(t, a.Before(b), "a: %v, b: %v", a, b)
	require.Truef(t, b.Before(c), "b: %v, c: %v", b, c)
	require.Truef(t, c.Before(d), "c: %v, d: %v", c, d)
	require.Truef(t, d.Before(e), "d: %v, e: %v", d, e)
	require.Truef(t, b.Sub(nowB) < 100*time.Millisecond, "b: %v, nowB: %v", b, nowB)
	// ensure now is the time after blocking
	require.Truef(t, e.Sub(nowE) < 100*time.Millisecond, "e: %v, nowE: %v", e, nowE)
}

func TestGormDuplicateCreate(t *testing.T) {
	gormDebug = true
	defer func() {
		gormDebug = false
	}()

	ctx := context.Background()
	driver, err := InitGormDriver(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	limiter := New(driver)

	key := "TestGormDuplicateCreate"
	durationPerToken := 100 * time.Millisecond
	burst := 10

	sig := make(chan struct{})
	var afterQueryCount atomic.Int64
	afterQuery := func(kv kvWrapper) {
		if afterQueryCount.Add(1) == 2 {
			close(sig)
		}
		<-sig
	}

	var errG errgroup.Group
	errG.Go(func() error {
		ctx := context.WithValue(ctx, ctxKeyAfterQuery{}, afterQuery)
		r, err := limiter.Reserve(ctx, &ReserveRequest{
			Key:              key,
			DurationPerToken: durationPerToken,
			Burst:            burst,
			Now:              time.Time{},
			Tokens:           5,
			MaxFutureReserve: 0,
		})
		if err != nil {
			return err
		}
		require.True(t, r.OK)
		return nil
	})
	errG.Go(func() error {
		ctx := context.WithValue(ctx, ctxKeyAfterQuery{}, afterQuery)
		r, err := limiter.Reserve(ctx, &ReserveRequest{
			Key:              key,
			DurationPerToken: durationPerToken,
			Burst:            burst,
			Now:              time.Time{},
			Tokens:           5,
			MaxFutureReserve: 0,
		})
		if err != nil {
			return err
		}
		require.True(t, r.OK)
		return nil
	})
	if err := errG.Wait(); err != nil {
		t.Fatal(err)
	}
}
