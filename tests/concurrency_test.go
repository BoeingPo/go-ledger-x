package tests

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/boeing/go-ledger-x/internal/domain"
	"github.com/boeing/go-ledger-x/internal/repository"
)

func newTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set — skipping integration test")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("connect to test db: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

var keyCounter atomic.Int64

func newKey() string {
	return fmt.Sprintf("test-key-%d", keyCounter.Add(1))
}

func newUserID() int64 {
	return rand.Int64N(1<<62) + 1
}

// TestConcurrentDebits fires 100 simultaneous debits against a single wallet
// and verifies the final balance is exact — no race conditions, no overdraft.
func TestConcurrentDebits(t *testing.T) {
	ctx := context.Background()
	repo := repository.NewWalletRepository(newTestPool(t))

	const (
		workers       = 100
		debitAmount   = 1000
		initialCredit = workers * debitAmount * 2 // fund double so all succeed
	)

	wallet, err := repo.CreateWallet(ctx, newUserID())
	if err != nil {
		t.Fatalf("create wallet: %v", err)
	}

	_, err = repo.Credit(ctx, domain.CreditRequest{
		IdempotencyKey: newKey(),
		WalletID:       wallet.ID,
		Amount:         initialCredit,
	})
	if err != nil {
		t.Fatalf("seed credit: %v", err)
	}

	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		successes int
		failures  int
	)

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := repo.Debit(ctx, domain.DebitRequest{
				IdempotencyKey: newKey(),
				WalletID:       wallet.ID,
				Amount:         debitAmount,
			})
			mu.Lock()
			if err == nil {
				successes++
			} else {
				failures++
			}
			mu.Unlock()
		}()
	}
	wg.Wait()

	updated, err := repo.GetWalletByUserID(ctx, wallet.UserID)
	if err != nil {
		t.Fatalf("read final balance: %v", err)
	}

	expected := int64(initialCredit - successes*debitAmount)
	if updated.Balance != expected {
		t.Errorf("balance mismatch: got %d, want %d (successes=%d failures=%d)",
			updated.Balance, expected, successes, failures)
	}
	if updated.Balance < 0 {
		t.Errorf("balance went negative: %d", updated.Balance)
	}

	t.Logf("100 concurrent debits: %d succeeded, %d failed, final balance=%d",
		successes, failures, updated.Balance)
}

// TestIdempotency verifies that submitting the same idempotency key twice
// does not double-apply the transaction.
func TestIdempotency(t *testing.T) {
	ctx := context.Background()
	repo := repository.NewWalletRepository(newTestPool(t))

	wallet, err := repo.CreateWallet(ctx, newUserID())
	if err != nil {
		t.Fatalf("create wallet: %v", err)
	}

	req := domain.CreditRequest{
		IdempotencyKey: newKey(),
		WalletID:       wallet.ID,
		Amount:         50000, // 5.0000
	}

	tx1, err := repo.Credit(ctx, req)
	if err != nil {
		t.Fatalf("first credit: %v", err)
	}

	tx2, err := repo.Credit(ctx, req)
	if err != nil {
		t.Fatalf("second credit (idempotent): %v", err)
	}

	if tx1.ID != tx2.ID {
		t.Errorf("idempotent calls returned different transaction IDs: %d vs %d", tx1.ID, tx2.ID)
	}

	w, err := repo.GetWalletByUserID(ctx, wallet.UserID)
	if err != nil {
		t.Fatalf("read wallet: %v", err)
	}
	if w.Balance != 50000 {
		t.Errorf("balance should be 50000 (applied once), got %d", w.Balance)
	}
}

// TestTransferNoOverdraft verifies that concurrent transfers cannot drain a
// wallet below zero even when more is requested than the balance holds.
func TestTransferNoOverdraft(t *testing.T) {
	ctx := context.Background()
	repo := repository.NewWalletRepository(newTestPool(t))

	alice, err := repo.CreateWallet(ctx, newUserID())
	if err != nil {
		t.Fatalf("create alice: %v", err)
	}
	bob, err := repo.CreateWallet(ctx, newUserID())
	if err != nil {
		t.Fatalf("create bob: %v", err)
	}

	_, err = repo.Credit(ctx, domain.CreditRequest{
		IdempotencyKey: newKey(),
		WalletID:       alice.ID,
		Amount:         100000, // 10.0000
	})
	if err != nil {
		t.Fatalf("seed alice: %v", err)
	}

	const workers = 50
	var wg sync.WaitGroup

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			repo.Transfer(ctx, domain.TransferRequest{ //nolint:errcheck
				IdempotencyKey: newKey(),
				FromWalletID:   alice.ID,
				ToWalletID:     bob.ID,
				Amount:         5000, // 0.5000
			})
		}()
	}
	wg.Wait()

	a, err := repo.GetWalletByUserID(ctx, alice.UserID)
	if err != nil {
		t.Fatalf("read alice: %v", err)
	}
	b, err := repo.GetWalletByUserID(ctx, bob.UserID)
	if err != nil {
		t.Fatalf("read bob: %v", err)
	}

	if a.Balance < 0 {
		t.Errorf("alice balance went negative: %d", a.Balance)
	}
	if a.Balance+b.Balance != 100000 {
		t.Errorf("money not conserved: alice=%d bob=%d sum=%d want=100000",
			a.Balance, b.Balance, a.Balance+b.Balance)
	}

	t.Logf("transfer test: alice=%d bob=%d", a.Balance, b.Balance)
}
