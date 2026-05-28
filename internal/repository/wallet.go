package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/boeing/go-ledger-x/internal/domain"
)

var (
	ErrWalletNotFound      = errors.New("wallet not found")
	ErrInsufficientBalance = errors.New("insufficient balance")
)

type WalletRepository struct {
	pool *pgxpool.Pool
}

func NewWalletRepository(pool *pgxpool.Pool) *WalletRepository {
	return &WalletRepository{pool: pool}
}

func (r *WalletRepository) CreateWallet(ctx context.Context, userID int64) (*domain.Wallet, error) {
	var w domain.Wallet
	err := r.pool.QueryRow(ctx, `
		INSERT INTO ledger.wallets (user_id)
		VALUES ($1)
		ON CONFLICT (user_id) DO UPDATE SET user_id = EXCLUDED.user_id
		RETURNING id, user_id, balance, created_at, updated_at
	`, userID).Scan(&w.ID, &w.UserID, &w.Balance, &w.CreatedAt, &w.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("create wallet: %w", err)
	}
	return &w, nil
}

func (r *WalletRepository) GetWalletByUserID(ctx context.Context, userID int64) (*domain.Wallet, error) {
	var w domain.Wallet
	err := r.pool.QueryRow(ctx, `
		SELECT id, user_id, balance, created_at, updated_at
		FROM ledger.wallets WHERE user_id = $1
	`, userID).Scan(&w.ID, &w.UserID, &w.Balance, &w.CreatedAt, &w.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrWalletNotFound
		}
		return nil, fmt.Errorf("get wallet: %w", err)
	}
	return &w, nil
}

func (r *WalletRepository) GetWalletByID(ctx context.Context, walletID int64) (*domain.Wallet, error) {
	var w domain.Wallet
	err := r.pool.QueryRow(ctx, `
		SELECT id, user_id, balance, created_at, updated_at
		FROM ledger.wallets WHERE id = $1
	`, walletID).Scan(&w.ID, &w.UserID, &w.Balance, &w.CreatedAt, &w.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrWalletNotFound
		}
		return nil, fmt.Errorf("get wallet by id: %w", err)
	}
	return &w, nil
}

func (r *WalletRepository) Credit(ctx context.Context, req domain.CreditRequest) (*domain.Transaction, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	existing, err := checkIdempotency(ctx, tx, req.IdempotencyKey)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		return existing, nil
	}

	// Lock the wallet row before modifying balance.
	var balance int64
	err = tx.QueryRow(ctx, `
		SELECT balance FROM ledger.wallets WHERE id = $1 FOR UPDATE
	`, req.WalletID).Scan(&balance)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrWalletNotFound
		}
		return nil, fmt.Errorf("lock wallet: %w", err)
	}

	if _, err = tx.Exec(ctx, `
		UPDATE ledger.wallets SET balance = balance + $1, updated_at = NOW() WHERE id = $2
	`, req.Amount, req.WalletID); err != nil {
		return nil, fmt.Errorf("credit balance: %w", err)
	}

	t, err := insertTransaction(ctx, tx, req.IdempotencyKey, nil, &req.WalletID, req.Amount, domain.TypeCredit)
	if err != nil {
		return nil, err
	}

	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit credit: %w", err)
	}
	return t, nil
}

func (r *WalletRepository) Debit(ctx context.Context, req domain.DebitRequest) (*domain.Transaction, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	existing, err := checkIdempotency(ctx, tx, req.IdempotencyKey)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		return existing, nil
	}

	var balance int64
	err = tx.QueryRow(ctx, `
		SELECT balance FROM ledger.wallets WHERE id = $1 FOR UPDATE
	`, req.WalletID).Scan(&balance)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrWalletNotFound
		}
		return nil, fmt.Errorf("lock wallet: %w", err)
	}

	if balance < req.Amount {
		return nil, ErrInsufficientBalance
	}

	if _, err = tx.Exec(ctx, `
		UPDATE ledger.wallets SET balance = balance - $1, updated_at = NOW() WHERE id = $2
	`, req.Amount, req.WalletID); err != nil {
		return nil, fmt.Errorf("debit balance: %w", err)
	}

	t, err := insertTransaction(ctx, tx, req.IdempotencyKey, &req.WalletID, nil, req.Amount, domain.TypeDebit)
	if err != nil {
		return nil, err
	}

	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit debit: %w", err)
	}
	return t, nil
}

func (r *WalletRepository) Transfer(ctx context.Context, req domain.TransferRequest) (*domain.Transaction, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	existing, err := checkIdempotency(ctx, tx, req.IdempotencyKey)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		return existing, nil
	}

	// Lock both wallets in ascending ID order to prevent deadlock when two
	// concurrent transfers go in opposite directions (A→B and B→A).
	lockFirst, lockSecond := req.FromWalletID, req.ToWalletID
	if lockFirst > lockSecond {
		lockFirst, lockSecond = lockSecond, lockFirst
	}

	balances := make(map[int64]int64, 2)
	for _, id := range [2]int64{lockFirst, lockSecond} {
		var bal int64
		if err = tx.QueryRow(ctx, `
			SELECT balance FROM ledger.wallets WHERE id = $1 FOR UPDATE
		`, id).Scan(&bal); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, ErrWalletNotFound
			}
			return nil, fmt.Errorf("lock wallet %d: %w", id, err)
		}
		balances[id] = bal
	}

	if balances[req.FromWalletID] < req.Amount {
		return nil, ErrInsufficientBalance
	}

	if _, err = tx.Exec(ctx, `
		UPDATE ledger.wallets SET balance = balance - $1, updated_at = NOW() WHERE id = $2
	`, req.Amount, req.FromWalletID); err != nil {
		return nil, fmt.Errorf("debit from wallet: %w", err)
	}

	if _, err = tx.Exec(ctx, `
		UPDATE ledger.wallets SET balance = balance + $1, updated_at = NOW() WHERE id = $2
	`, req.Amount, req.ToWalletID); err != nil {
		return nil, fmt.Errorf("credit to wallet: %w", err)
	}

	t, err := insertTransaction(ctx, tx, req.IdempotencyKey, &req.FromWalletID, &req.ToWalletID, req.Amount, domain.TypeTransfer)
	if err != nil {
		return nil, err
	}

	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit transfer: %w", err)
	}
	return t, nil
}

func checkIdempotency(ctx context.Context, tx pgx.Tx, key string) (*domain.Transaction, error) {
	var t domain.Transaction
	err := tx.QueryRow(ctx, `
		SELECT id, idempotency_key, from_wallet_id, to_wallet_id, amount, type, status, created_at
		FROM ledger.transactions WHERE idempotency_key = $1
	`, key).Scan(&t.ID, &t.IdempotencyKey, &t.FromWalletID, &t.ToWalletID, &t.Amount, &t.Type, &t.Status, &t.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("check idempotency: %w", err)
	}
	return &t, nil
}

func insertTransaction(ctx context.Context, tx pgx.Tx, key string, from, to *int64, amount int64, txType domain.TransactionType) (*domain.Transaction, error) {
	var t domain.Transaction
	err := tx.QueryRow(ctx, `
		INSERT INTO ledger.transactions (idempotency_key, from_wallet_id, to_wallet_id, amount, type, status)
		VALUES ($1, $2, $3, $4, $5, 'completed')
		RETURNING id, idempotency_key, from_wallet_id, to_wallet_id, amount, type, status, created_at
	`, key, from, to, amount, txType).Scan(
		&t.ID, &t.IdempotencyKey, &t.FromWalletID, &t.ToWalletID,
		&t.Amount, &t.Type, &t.Status, &t.CreatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("insert transaction: %w", err)
	}
	return &t, nil
}
