package domain

import "time"

// BalanceScale is the fixed-point multiplier: 1.0000 is stored as 10000.
const BalanceScale int64 = 10000

type Wallet struct {
	ID        int64
	UserID    int64
	Balance   int64 // fixed-point integer, divide by BalanceScale to get decimal
	CreatedAt time.Time
	UpdatedAt time.Time
}
