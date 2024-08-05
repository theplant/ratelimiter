package ratelimiter

import (
	"context"
	"strconv"
	"time"

	"github.com/pkg/errors"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type KV struct {
	Key   string `json:"key" gorm:"primaryKey;not null;"`
	Value string `json:"value" gorm:"not null;"`
}

func DriverGORM(db *gorm.DB) Driver {
	return DriverFunc(func(ctx context.Context, req *ReserveRequest) (*Reservation, error) {
		now := req.Now.UTC() // stripMono
		if req.Key == "" || now.IsZero() || req.DurationPerToken <= 0 || req.Burst <= 0 || req.Tokens <= 0 || req.Tokens > req.Burst {
			return nil, errors.Wrapf(ErrInvalidParameters, "%v", req)
		}

		select {
		case <-ctx.Done():
			return nil, errors.Wrap(ctx.Err(), "ratelimiter: context done")
		default:
		}

		resetValue := now.Add(-time.Duration(req.Burst) * req.DurationPerToken)

		var timeBase time.Time
		var timeToAct time.Time
		var ok bool

		err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			var kv KV

			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&kv, "key = ?", req.Key).Error; err != nil {
				if !errors.Is(err, gorm.ErrRecordNotFound) {
					return errors.Wrap(err, "ratelimiter: failed to get kv")
				}

				timeBase = resetValue
				kv = KV{Key: req.Key, Value: strconv.FormatInt(timeBase.UnixMicro(), 10)}
				if err := tx.Create(&kv).Error; err != nil {
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

			kv.Value = strconv.FormatInt(timeToAct.UnixMicro(), 10)
			if err := tx.Save(&kv).Error; err != nil {
				return errors.Wrap(err, "ratelimiter: failed to save time to act")
			}
			ok = true
			return nil
		})
		if err != nil {
			return nil, err
		}

		return &Reservation{
			ReserveRequest: req,
			OK:             ok,
			TimeToAct:      timeToAct,
		}, nil
	})
}
