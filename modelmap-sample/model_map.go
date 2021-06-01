package models

import (
	"time"

	"github.com/shopspring/decimal"
	"github.com/warrior21st/go-utils/commonutil"
	"github.com/warrior21st/go-utils/timeutil"
)

type LpRecord struct {
	ID                  int64
	USER_ADDRESS        string
	TOKEN_NAME          string
	TOKEN_AMOUNT        decimal.Decimal
	RM_LP_SIGN_DEADLINE int64
	CREATE_TIME         time.Time
}

func MapLpRecord(row map[string]string) *LpRecord {
	return &LpRecord{
		ID:                  commonutil.ParseInt64(row["ID"]),
		USER_ADDRESS:        string(row["USER_ADDRESS"]),
		TOKEN_NAME:          string(row["TOKEN_NAME"]),
		TOKEN_AMOUNT:        commonutil.ParseDecimal(row["TOKEN_AMOUNT"]),
		RM_LP_SIGN_DEADLINE: commonutil.ParseInt64(row["RM_LP_SIGN_DEADLINE"]),
		CREATE_TIME:         *timeutil.StringToTime(row["CREATE_TIME"]),
	}
}