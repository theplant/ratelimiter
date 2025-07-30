package ratelimiter

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pkg/errors"
	"gorm.io/gorm"
)

var (
	// MaxMigrationAttempts defines the maximum number of retry attempts for migration
	MaxMigrationAttempts = 5
	// ColumnKey is the fixed column name for rate limit keys
	ColumnKey = "key"
	// ColumnTimeToAct is the fixed column name for storing time to act (unix microseconds)
	ColumnTimeToAct = "time_to_act"
)

// SQLRateLimiter implements the RateLimiter interface using GORM for SQL database storage.
// It provides rate limiting functionality with standardized table structure,
// and uses row-level locking (FOR UPDATE) to ensure consistency in concurrent scenarios.
type SQLRateLimiter struct {
	db          *gorm.DB
	tableName   string
	rawQuery    string
	insertQuery string
	updateQuery string
}

// Ensure SQLRateLimiter implements the RateLimiter interface
var _ RateLimiter = &SQLRateLimiter{}

// NewSQLRateLimiter creates a new SQLRateLimiter with the provided database and table name.
func NewSQLRateLimiter(db *gorm.DB, tableName string) (*SQLRateLimiter, error) {
	if db == nil {
		return nil, errors.New("DB is nil")
	}
	if tableName == "" {
		return nil, errors.New("tableName is empty")
	}

	s := &SQLRateLimiter{
		db:        db,
		tableName: tableName,
	}

	// Build raw query based on database type
	// Prioritize real-time accuracy for precise rate limiting
	var currentTimestampQuery string
	switch db.Dialector.Name() {
	case "mysql":
		currentTimestampQuery = "SYSDATE(6)" // MySQL: real-time timestamp with microsecond precision
	case "postgres":
		currentTimestampQuery = "clock_timestamp()" // PostgreSQL: real-time timestamp for maximum accuracy
	case "sqlite":
		currentTimestampQuery = "datetime('now', 'subsec')" // SQLite: statement-level real-time timestamp
	default:
		currentTimestampQuery = "CURRENT_TIMESTAMP"
	}

	s.rawQuery = fmt.Sprintf(`
	WITH kv_select AS (
		SELECT %s, %s FROM %s WHERE %s = ? FOR UPDATE
	)
	SELECT kv.%s, kv.%s, %s AS now 
	FROM (SELECT 1) AS dummy
	LEFT JOIN kv_select AS kv ON kv.%s = ?;
	`, ColumnKey, ColumnTimeToAct, tableName, ColumnKey, ColumnKey, ColumnTimeToAct, currentTimestampQuery, ColumnKey)

	s.insertQuery = fmt.Sprintf(`
		INSERT INTO %s (%s, %s) VALUES (?, ?)
	`, s.tableName, ColumnKey, ColumnTimeToAct)

	s.updateQuery = fmt.Sprintf(`
		UPDATE %s SET %s = ? WHERE %s = ?
	`, s.tableName, ColumnTimeToAct, ColumnKey)
	return s, nil
}

// Allow checks if the specified number of tokens are available immediately.
// It returns true if the request can be satisfied without waiting, false otherwise.
// This method is implemented as a Reserve operation with MaxFutureReserve set to 0.
func (s *SQLRateLimiter) Allow(ctx context.Context, req *AllowRequest) (bool, error) {
	return Allow(ctx, s, req)
}

// Migrate creates the required table if it doesn't exist.
// This method follows the database design standards and handles concurrent migration attempts.
// It's safe to call this method multiple times - it will only create the table if it doesn't exist.
//
// The created table structure follows these principles:
// - Uses VARCHAR(255) for key column to provide format flexibility
// - Uses BIGINT for time storage (unix microseconds)
// - No foreign keys (logical relationships only)
// - Optimized for rate limiting workloads
func (s *SQLRateLimiter) Migrate(ctx context.Context) error {
	// Build CREATE TABLE statement according to database design standards
	createTableSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			%s VARCHAR(255) PRIMARY KEY NOT NULL,
			%s BIGINT NOT NULL
		)
	`, s.tableName, ColumnKey, ColumnTimeToAct)

	// Retry mechanism to handle concurrent table creation
	for attempts := 0; attempts < MaxMigrationAttempts; attempts++ {
		err := s.db.WithContext(ctx).Exec(createTableSQL).Error
		if err == nil {
			return nil
		}

		if attempts == MaxMigrationAttempts-1 {
			return errors.Wrap(err, "failed to create table")
		}

		errMsg := err.Error()
		// Handle PostgreSQL concurrent table creation conflicts
		if strings.Contains(errMsg, `duplicate key value violates unique constraint`) ||
			strings.Contains(errMsg, "already exists (SQLSTATE 42P07)") {
			select {
			case <-ctx.Done():
				return errors.Wrap(ctx.Err(), "migration cancelled during backoff")
			case <-time.After(time.Duration(100+rand.Intn(100)) * time.Millisecond):
			}
			continue
		}

		return errors.Wrap(err, "failed to create table")
	}
	return nil
}

type kvWrapper struct {
	Key       string `gorm:"column:key"`
	TimeToAct int64  `gorm:"column:time_to_act"`
	Now       time.Time
}

type ctxKeyAfterQuery struct{}

func isDuplicateKeyError(err error) bool {
	if err == nil {
		return false
	}

	errMsg := err.Error()

	// Check for MySQL duplicate key errors
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
		return true
	}

	// Check for PostgreSQL duplicate key errors
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return true
	}

	// Fallback to string matching for other database libraries or wrapped errors
	return strings.Contains(errMsg, "duplicate") ||
		strings.Contains(errMsg, "UNIQUE constraint failed") ||
		strings.Contains(errMsg, "already exists")
}

// Reserve attempts to reserve the specified number of tokens.
// It returns a Reservation indicating whether the request was successful
// and when the action should be performed.
func (s *SQLRateLimiter) Reserve(ctx context.Context, req *ReserveRequest) (*Reservation, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	return s.attempt(ctx, req, 0)
}

func (s *SQLRateLimiter) attempt(ctx context.Context, req *ReserveRequest, idx int) (*Reservation, error) {
	select {
	case <-ctx.Done():
		return nil, errors.Wrap(ctx.Err(), "context done")
	default:
	}

	var now time.Time
	if isTestMode(ctx) {
		nowFunc, exists := nowFuncFromContextForTest(ctx)
		if exists {
			now = nowFunc().UTC() // stripMono
		}
	}

	var timeBase time.Time
	var timeToAct time.Time
	var ok bool

	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var kv kvWrapper

		if err := tx.Raw(s.rawQuery, req.Key, req.Key).Scan(&kv).Error; err != nil {
			return errors.Wrap(err, "failed to get kv")
		}

		if isTestMode(ctx) {
			afterQuery, ok := ctx.Value(ctxKeyAfterQuery{}).(func(kv kvWrapper))
			if ok {
				afterQuery(kv)
			}
		}

		if now.IsZero() {
			now = kv.Now // use db time
		}
		resetValue := now.Add(-time.Duration(req.Burst) * req.DurationPerToken)

		if kv.Key == "" { // not found
			timeBase = resetValue
			if err := tx.Exec(s.insertQuery, req.Key, timeBase.UnixMicro()).Error; err != nil {
				return errors.Wrap(err, "failed to create kv")
			}
		} else {
			timeBase = time.UnixMicro(kv.TimeToAct)
			if timeBase.Before(resetValue) {
				timeBase = resetValue
			}
		}

		timeToAct = timeBase.Add(time.Duration(req.Tokens) * req.DurationPerToken)

		if timeToAct.After(now.Add(req.MaxFutureReserve)) {
			ok = false
			return nil
		}

		if err := tx.Exec(s.updateQuery, timeToAct.UnixMicro(), req.Key).Error; err != nil {
			return errors.Wrap(err, "failed to save time to act")
		}

		ok = true
		return nil
	})
	if err != nil {
		// retry once if duplicate key error
		if idx == 0 && isDuplicateKeyError(err) {
			return s.attempt(ctx, req, idx+1)
		}
		return nil, err
	}

	return &Reservation{
		ReserveRequest: req,
		OK:             ok,
		TimeToAct:      timeToAct,
		ReservedAt:     now,
	}, nil
}
