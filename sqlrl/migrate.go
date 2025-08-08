package sqlrl

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/pkg/errors"
	"gorm.io/gorm"
)

// Migrate creates the required table if it doesn't exist.
// This method follows the database design standards and handles concurrent migration attempts.
// It's safe to call this method multiple times - it will only create the table if it doesn't exist.
//
// The created table structure follows these principles:
// - Uses VARCHAR(255) for key column to provide format flexibility
// - Uses BIGINT for time storage (unix microseconds)
// - No foreign keys (logical relationships only)
// - Optimized for rate limiting workloads
func Migrate(ctx context.Context, db *gorm.DB, tableName string) error {
	// Build CREATE TABLE statement according to database design standards
	createTableSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			%s VARCHAR(255) PRIMARY KEY NOT NULL,
			%s BIGINT NOT NULL
		)
	`, tableName, ColumnKey, ColumnTimeToAct)

	// Retry mechanism to handle concurrent table creation
	for attempts := 0; attempts < MaxMigrationAttempts; attempts++ {
		err := db.WithContext(ctx).Exec(createTableSQL).Error
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
