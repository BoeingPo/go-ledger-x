package domain

import "time"

type TransactionType   string
type TransactionStatus string

const (
	TypeCredit   TransactionType = "credit"
	TypeDebit    TransactionType = "debit"
	TypeTransfer TransactionType = "transfer"

	StatusPending   TransactionStatus = "pending"
	StatusCompleted TransactionStatus = "completed"
	StatusFailed    TransactionStatus = "failed"
)

type Transaction struct {
	ID             int64
	IdempotencyKey string
	FromWalletID   *int64
	ToWalletID     *int64
	Amount         int64
	Type           TransactionType
	Status         TransactionStatus
	CreatedAt      time.Time
}

type CreditRequest struct {
	IdempotencyKey string
	WalletID       int64
	Amount         int64
}

type DebitRequest struct {
	IdempotencyKey string
	WalletID       int64
	Amount         int64
}

type TransferRequest struct {
	IdempotencyKey string
	FromWalletID   int64
	ToWalletID     int64
	Amount         int64
}
