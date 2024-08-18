package ratelimiter

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pkg/errors"
	"gorm.io/gorm"
)

type KV struct {
	Key   string `json:"key" gorm:"primaryKey;not null;"`
	Value string `json:"value" gorm:"not null;"`
}

type GormDriver struct {
	db       *gorm.DB
	rawQuery string
}

// NewGormDriver returns a Driver that uses Gorm as the storage.
// Sometimes you may need to auto migrate the KV table, you can use `InitGormDriver` instead.
func NewGormDriver(db *gorm.DB) *GormDriver {
	d := &GormDriver{
		db: db,
	}

	var currentTimestampQuery string
	switch db.Dialector.Name() {
	case "mysql":
		currentTimestampQuery = "CURRENT_TIMESTAMP(6)"
	case "postgres":
		currentTimestampQuery = "clock_timestamp()"
	default:
		// Fallback to a generic solution or handle other databases if needed
		currentTimestampQuery = "CURRENT_TIMESTAMP"
	}

	d.rawQuery = fmt.Sprintf(`
	WITH kv_select AS (
		SELECT * FROM kvs WHERE key = ? FOR UPDATE
	)
	SELECT kv.*, %s AS now 
	FROM (SELECT 1) AS dummy
	LEFT JOIN kv_select AS kv ON kv.key = ?;
	`, currentTimestampQuery)
	return d
}

// InitGormDriver initializes a GormDriver with the provided Gorm DB.
// Sometimes you may not need to auto migrate the KV table, you can use `NewGormDriver` instead.
func InitGormDriver(ctx context.Context, db *gorm.DB) (*GormDriver, error) {
	if err := db.AutoMigrate(&KV{}); err != nil {
		return nil, errors.Wrap(err, "ratelimiter: failed to migrate kv")
	}

	return NewGormDriver(db), nil
}

type kvWrapper struct {
	KV
	Now time.Time
}

type ctxKeyAfterQuery struct{}

func isDuplicateKeyError(err error) bool {
	if err == nil {
		return false
	}

	var pqErr *pgconn.PgError
	if errors.As(err, &pqErr) {
		return pqErr.Code == "23505"
	}

	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		return mysqlErr.Number == 1062
	}

	errMsg := err.Error()
	return strings.Contains(errMsg, "SQLSTATE 23505")
}

func (d *GormDriver) Reserve(ctx context.Context, req *ReserveRequest) (*Reservation, error) {
	return d.reserve(ctx, req, 0)
}

func (d *GormDriver) reserve(ctx context.Context, req *ReserveRequest, idx int) (*Reservation, error) {
	if req.Key == "" || req.DurationPerToken <= 0 || req.Burst <= 0 || req.Tokens <= 0 || req.Tokens > req.Burst {
		return nil, errors.Wrapf(ErrInvalidParameters, "%v", req)
	}

	select {
	case <-ctx.Done():
		return nil, errors.Wrap(ctx.Err(), "ratelimiter: context done")
	default:
	}

	var now time.Time
	if Test {
		nowFunc, exists := NowFuncFromContextForTest(ctx)
		if exists {
			now = nowFunc().UTC() // stripMono
		}
	}

	var timeBase time.Time
	var timeToAct time.Time
	var ok bool

	err := d.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var kv kvWrapper

		if err := tx.Raw(d.rawQuery, req.Key, req.Key).Scan(&kv).Error; err != nil {
			return errors.Wrap(err, "ratelimiter: failed to get kv")
		}

		if Test {
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
			if err := tx.Create(&KV{
				Key:   req.Key,
				Value: strconv.FormatInt(timeBase.UnixMicro(), 10),
			}).Error; err != nil {
				return errors.Wrap(err, "ratelimiter: failed to create kv")
			}
		} else {
			unixMicroBase, err := strconv.ParseInt(kv.Value, 10, 64)
			if err != nil {
				return errors.Wrap(err, "ratelimiter: failed to parse base time")
			}
			timeBase = time.UnixMicro(unixMicroBase)

			if timeBase.Before(resetValue) {
				timeBase = resetValue
			}
		}

		tokensDuration := req.DurationPerToken * time.Duration(req.Tokens)
		timeToAct = timeBase.Add(tokensDuration).UTC()

		if timeToAct.After(now.Add(req.MaxFutureReserve)) {
			ok = false
			return nil
		}

		if err := tx.Model(&KV{}).Where("key = ?", req.Key).Update(
			"value", strconv.FormatInt(timeToAct.UnixMicro(), 10),
		).Error; err != nil {
			return errors.Wrap(err, "ratelimiter: failed to save time to act")
		}
		ok = true
		return nil
	})
	if err != nil {
		// retry once if duplicate key errorf
		if idx == 0 && isDuplicateKeyError(err) {
			return d.reserve(ctx, req, idx+1)
		}
		return nil, err
	}

	return &Reservation{
		ReserveRequest: req,
		OK:             ok,
		TimeToAct:      timeToAct,
		Now:            now,
	}, nil
}
