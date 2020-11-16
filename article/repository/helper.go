package repository

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"time"
)

const (
	timeFormat = "2006-01-02T15:04:05.999Z07:00" // reduce precision from RFC3339Nano as date format
)

// DecodeCursor will decode cursor from user for mysql
func DecodeCursor(encodedTime string) (time.Time, error) {
	byt, err := base64.StdEncoding.DecodeString(encodedTime)
	if err != nil {
		return time.Time{}, err
	}

	timeString := string(byt)
	t, err := time.Parse(timeFormat, timeString)

	return t, err
}

// EncodeCursor will encode cursor from mysql to user
func EncodeCursor(t time.Time) string {
	timeString := t.Format(timeFormat)

	return base64.StdEncoding.EncodeToString([]byte(timeString))
}

type contextKey string

const ctxKeyTransaction contextKey = "dbTx"

func GetTransactionFromCtx(ctx context.Context) (*sql.Tx, bool) {
	tx, ok := ctx.Value(ctxKeyTransaction).(*sql.Tx)
	return tx, ok
}

func newOrNestedTx(ctx context.Context, dbPG *sql.DB) (tx *sql.Tx, isNestedTx bool) {
	tx, isNestedTx = GetTransactionFromCtx(ctx)
	if !isNestedTx {
		tx, _ = dbPG.BeginTx(ctx, nil)
	}
	return
}

func recoverRollbackOrCommit(tx *sql.Tx, err *error) {
	if r := recover(); r != nil {
		switch x := r.(type) {
		case string:
			*err = errors.New(x)
		case error:
			*err = x
		default:
			*err = errors.New("unknown error")
		}
		tx.Rollback()
	} else if *err != nil {
		tx.Rollback()
	} else {
		tx.Commit()
	}
}

func WithTransaction(ctx context.Context, db *sql.DB, fn func(context.Context) error) (err error) {
	tx, isNestedTx := newOrNestedTx(ctx, db)
	if !isNestedTx {
		defer recoverRollbackOrCommit(tx, &err)
		ctx = context.WithValue(ctx, ctxKeyTransaction, tx)
	}
	err = fn(ctx)
	return
}
