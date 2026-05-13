package sqlrl

import (
	"context"
	"log"
	"sync/atomic"
	"testing"
	"time"

	"github.com/qor5/x/v3/gormx"
	"github.com/stretchr/testify/require"
	"github.com/theplant/ratelimiter"
	"golang.org/x/sync/errgroup"
	"gorm.io/gorm"
)

var db *gorm.DB

func TestMain(m *testing.M) {
	ctx := context.Background()
	suite := gormx.MustStartRawTestSuite(ctx)
	defer func() { _ = suite.Stop(ctx) }()
	db = suite.DB()

	// Create SQL rate limiter and migrate table
	sqlLimiter, err := New(db, "kvs")
	if err != nil {
		log.Fatalf("Failed to create SQL rate limiter: %v", err)
	}

	// Use Migrate method to create the table
	if err := sqlLimiter.Migrate(ctx); err != nil {
		log.Fatalf("Failed to migrate table: %v", err)
	}

	m.Run()
}

func TestSQLRateLimiterForUpdate(t *testing.T) {
	ctx := ratelimiter.WithTestMode(context.Background())
	limiter, err := New(db, "kvs")
	require.NoError(t, err)

	key := "TestSQLForUpdate"
	durationPerToken := 100 * time.Millisecond
	burst := 10

	// Initial setup - consume some tokens
	{
		r, err := limiter.Reserve(ctx, &ratelimiter.ReserveRequest{
			Key:              key,
			DurationPerToken: durationPerToken,
			Burst:            burst,
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
		r, err := limiter.Reserve(ctx, &ratelimiter.ReserveRequest{
			Key:              key,
			DurationPerToken: durationPerToken,
			Burst:            burst,
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
		r, err := limiter.Reserve(ctx, &ratelimiter.ReserveRequest{
			Key:              key,
			DurationPerToken: durationPerToken,
			Burst:            burst,
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
	t.Logf("c: %v", c)
	t.Logf("d: %v", d)
	t.Logf("e: %v", e)
	t.Logf("nowB: %v", nowB)
	t.Logf("nowE: %v", nowE)

	// b is after query, c is the second goroutine start time, d is the first goroutine end
	// e is the second after query time
	// the second goroutine should not be able to query before the first goroutine done
	require.True(t, c.Before(d), "c.Before(d): %v < %v", c, d)
	require.True(t, e.After(d), "e.After(d): %v > %v", e, d)
}

func TestSQLRateLimiterDuplicateCreate(t *testing.T) {
	ctx := ratelimiter.WithTestMode(context.Background())
	limiter, err := New(db, "kvs")
	require.NoError(t, err)

	key := "TestSQLDuplicateCreate"
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
		r, err := limiter.Reserve(ctx, &ratelimiter.ReserveRequest{
			Key:              key,
			DurationPerToken: durationPerToken,
			Burst:            burst,
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
		r, err := limiter.Reserve(ctx, &ratelimiter.ReserveRequest{
			Key:              key,
			DurationPerToken: durationPerToken,
			Burst:            burst,
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

func TestSQLRateLimiterCustomTable(t *testing.T) {
	ctx := context.Background()

	// Create SQL rate limiter with custom table name
	limiter, err := New(db, "custom_rate_limits")
	require.NoError(t, err)

	// Create table
	err = limiter.Migrate(ctx)
	require.NoError(t, err)

	key := "TestSQLCustomTable"
	durationPerToken := 100 * time.Millisecond
	burst := 5

	// Test that it works with custom table name
	r, err := limiter.Reserve(ctx, &ratelimiter.ReserveRequest{
		Key:              key,
		DurationPerToken: durationPerToken,
		Burst:            burst,
		Tokens:           1,
		MaxFutureReserve: 0,
	})
	require.NoError(t, err)
	require.True(t, r.OK)

	// Verify the key exists in the custom table
	var count int64
	err = db.Table("custom_rate_limits").Where("key = ?", key).Count(&count).Error
	require.NoError(t, err)
	require.Equal(t, int64(1), count, "Custom table should contain our test key")
}

func TestSQLRateLimiterMigrate(t *testing.T) {
	ctx := context.Background()

	// Test with a new table name that doesn't exist
	limiter, err := New(db, "migrate_test_table")
	require.NoError(t, err)

	// Migrate should create the table
	err = limiter.Migrate(ctx)
	require.NoError(t, err)

	// Verify table was created by checking if we can query it
	var count int64
	err = db.Table("migrate_test_table").Count(&count).Error
	require.NoError(t, err)
	require.Equal(t, int64(0), count, "New table should be empty")

	// Test that second migrate call is safe (idempotent)
	err = limiter.Migrate(ctx)
	require.NoError(t, err)

	// Test that rate limiting works with the migrated table
	reservation, err := limiter.Reserve(ctx, &ratelimiter.ReserveRequest{
		Key:              "test_migrate_key",
		Tokens:           1,
		Burst:            5,
		DurationPerToken: 100 * time.Millisecond,
		MaxFutureReserve: time.Second,
	})
	require.NoError(t, err)
	require.True(t, reservation.OK)

	// Verify data was written to the migrated table
	err = db.Table("migrate_test_table").Where("key = ?", "test_migrate_key").Count(&count).Error
	require.NoError(t, err)
	require.Equal(t, int64(1), count, "Migrated table should contain our test key")
}

func TestSQLRateLimiterConcurrentMigrate(t *testing.T) {
	ctx := context.Background()

	// Test concurrent migration attempts to ensure no race conditions
	tableName := "concurrent_migrate_test"

	// Start multiple goroutines trying to migrate simultaneously
	const numGoroutines = 5
	var errG errgroup.Group

	for i := 0; i < numGoroutines; i++ {
		errG.Go(func() error {
			limiter, err := New(db, tableName)
			if err != nil {
				return err
			}
			return limiter.Migrate(ctx)
		})
	}

	// All migrations should succeed without conflicts
	err := errG.Wait()
	require.NoError(t, err)

	// Verify table exists and is usable
	var count int64
	err = db.Table(tableName).Count(&count).Error
	require.NoError(t, err)
	require.Equal(t, int64(0), count, "Table should be empty after creation")

	// Test that the table is fully functional
	limiter, err := New(db, tableName)
	require.NoError(t, err)

	reservation, err := limiter.Reserve(ctx, &ratelimiter.ReserveRequest{
		Key:              "concurrent_test_key",
		Tokens:           1,
		Burst:            5,
		DurationPerToken: 100 * time.Millisecond,
		MaxFutureReserve: time.Second,
	})
	require.NoError(t, err)
	require.True(t, reservation.OK)
}

func TestNewSQLRateLimiterValidation(t *testing.T) {
	testCases := []struct {
		name        string
		db          interface{}
		tableName   string
		expectError bool
		errorText   string
	}{
		{
			name:        "nil db",
			db:          nil,
			tableName:   "test",
			expectError: true,
			errorText:   "DB is nil",
		},
		{
			name:        "empty tableName",
			db:          db,
			tableName:   "",
			expectError: true,
			errorText:   "tableName is empty",
		},
		{
			name:        "valid config",
			db:          db,
			tableName:   "test_table",
			expectError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var limiter *RateLimiter
			var err error

			if tc.db == nil {
				limiter, err = New(nil, tc.tableName)
			} else {
				limiter, err = New(db, tc.tableName)
			}

			if tc.expectError {
				require.Error(t, err)
				require.Nil(t, limiter)
				require.Contains(t, err.Error(), tc.errorText)
			} else {
				require.NoError(t, err)
				require.NotNil(t, limiter)
			}
		})
	}
}

func TestSQLRateLimiter_EdgeCases(t *testing.T) {
	ctx := context.Background()

	t.Run("migrate with context cancellation", func(t *testing.T) {
		limiter, err := New(db, "migrate_test_cancel")
		require.NoError(t, err)

		cancelCtx, cancel := context.WithCancel(ctx)
		cancel() // Cancel immediately

		err = limiter.Migrate(cancelCtx)
		// Depending on timing, this might succeed or fail
		// The important thing is that it doesn't panic
		if err != nil {
			require.Contains(t, err.Error(), "context")
		}
	})
}
