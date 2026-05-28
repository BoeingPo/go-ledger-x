package handler

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/boeing/go-ledger-x/internal/domain"
	"github.com/boeing/go-ledger-x/internal/repository"
	"github.com/boeing/go-ledger-x/internal/worker"
)

const jobTimeout = 30 * time.Second

type Handler struct {
	pool *worker.Pool
	repo *repository.WalletRepository
}

func New(pool *worker.Pool, repo *repository.WalletRepository) *Handler {
	return &Handler{pool: pool, repo: repo}
}

func (h *Handler) Routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", h.health)
	mux.HandleFunc("POST /api/ledger/wallets", h.createWallet)
	mux.HandleFunc("GET /api/ledger/wallets/{userID}", h.getWallet)
	mux.HandleFunc("POST /api/ledger/transactions/credit", h.credit)
	mux.HandleFunc("POST /api/ledger/transactions/debit", h.debit)
	mux.HandleFunc("POST /api/ledger/transactions/transfer", h.transfer)
	return mux
}

// --- health ---

func (h *Handler) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// --- wallets ---

func (h *Handler) createWallet(w http.ResponseWriter, r *http.Request) {
	var body struct {
		UserID int64 `json:"user_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}
	if body.UserID <= 0 {
		writeError(w, http.StatusBadRequest, "user_id must be a positive integer")
		return
	}
	wallet, err := h.repo.CreateWallet(r.Context(), body.UserID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, walletResponse(wallet))
}

func (h *Handler) getWallet(w http.ResponseWriter, r *http.Request) {
	userID, err := strconv.ParseInt(r.PathValue("userID"), 10, 64)
	if err != nil || userID <= 0 {
		writeError(w, http.StatusBadRequest, "invalid user_id")
		return
	}
	wallet, err := h.repo.GetWalletByUserID(r.Context(), userID)
	if err != nil {
		if errors.Is(err, repository.ErrWalletNotFound) {
			writeError(w, http.StatusNotFound, "wallet not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, walletResponse(wallet))
}

// --- transactions ---

func (h *Handler) credit(w http.ResponseWriter, r *http.Request) {
	var body struct {
		IdempotencyKey string `json:"idempotency_key"`
		WalletID       int64  `json:"wallet_id"`
		Amount         string `json:"amount"` // decimal string e.g. "10.5000"
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}
	amount, err := parseAmount(body.Amount)
	if err != nil || amount <= 0 {
		writeError(w, http.StatusBadRequest, "invalid amount")
		return
	}

	resultCh := make(chan worker.Result, 1)
	h.pool.Submit(worker.Job{
		Type:   worker.JobCredit,
		Credit: &domain.CreditRequest{IdempotencyKey: body.IdempotencyKey, WalletID: body.WalletID, Amount: amount},
		Result: resultCh,
	})

	select {
	case res := <-resultCh:
		if res.Err != nil {
			writeError(w, http.StatusUnprocessableEntity, res.Err.Error())
			return
		}
		writeJSON(w, http.StatusOK, txResponse(res.Transaction))
	case <-time.After(jobTimeout):
		writeError(w, http.StatusGatewayTimeout, "request timed out")
	}
}

func (h *Handler) debit(w http.ResponseWriter, r *http.Request) {
	var body struct {
		IdempotencyKey string `json:"idempotency_key"`
		WalletID       int64  `json:"wallet_id"`
		Amount         string `json:"amount"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}
	amount, err := parseAmount(body.Amount)
	if err != nil || amount <= 0 {
		writeError(w, http.StatusBadRequest, "invalid amount")
		return
	}

	resultCh := make(chan worker.Result, 1)
	h.pool.Submit(worker.Job{
		Type:  worker.JobDebit,
		Debit: &domain.DebitRequest{IdempotencyKey: body.IdempotencyKey, WalletID: body.WalletID, Amount: amount},
		Result: resultCh,
	})

	select {
	case res := <-resultCh:
		if res.Err != nil {
			if errors.Is(res.Err, repository.ErrInsufficientBalance) {
				writeError(w, http.StatusUnprocessableEntity, "insufficient balance")
				return
			}
			writeError(w, http.StatusUnprocessableEntity, res.Err.Error())
			return
		}
		writeJSON(w, http.StatusOK, txResponse(res.Transaction))
	case <-time.After(jobTimeout):
		writeError(w, http.StatusGatewayTimeout, "request timed out")
	}
}

func (h *Handler) transfer(w http.ResponseWriter, r *http.Request) {
	var body struct {
		IdempotencyKey string `json:"idempotency_key"`
		FromWalletID   int64  `json:"from_wallet_id"`
		ToWalletID     int64  `json:"to_wallet_id"`
		Amount         string `json:"amount"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}
	amount, err := parseAmount(body.Amount)
	if err != nil || amount <= 0 {
		writeError(w, http.StatusBadRequest, "invalid amount")
		return
	}

	resultCh := make(chan worker.Result, 1)
	h.pool.Submit(worker.Job{
		Type:     worker.JobTransfer,
		Transfer: &domain.TransferRequest{IdempotencyKey: body.IdempotencyKey, FromWalletID: body.FromWalletID, ToWalletID: body.ToWalletID, Amount: amount},
		Result:   resultCh,
	})

	select {
	case res := <-resultCh:
		if res.Err != nil {
			if errors.Is(res.Err, repository.ErrInsufficientBalance) {
				writeError(w, http.StatusUnprocessableEntity, "insufficient balance")
				return
			}
			writeError(w, http.StatusUnprocessableEntity, res.Err.Error())
			return
		}
		writeJSON(w, http.StatusOK, txResponse(res.Transaction))
	case <-time.After(jobTimeout):
		writeError(w, http.StatusGatewayTimeout, "request timed out")
	}
}

// --- response shapes ---

func walletResponse(w *domain.Wallet) map[string]any {
	return map[string]any{
		"id":         w.ID,
		"user_id":    w.UserID,
		"balance":    formatAmount(w.Balance),
		"created_at": w.CreatedAt,
		"updated_at": w.UpdatedAt,
	}
}

func txResponse(t *domain.Transaction) map[string]any {
	res := map[string]any{
		"id":              t.ID,
		"idempotency_key": t.IdempotencyKey,
		"amount":          formatAmount(t.Amount),
		"type":            t.Type,
		"status":          t.Status,
		"created_at":      t.CreatedAt,
	}
	if t.FromWalletID != nil {
		res["from_wallet_id"] = *t.FromWalletID
	}
	if t.ToWalletID != nil {
		res["to_wallet_id"] = *t.ToWalletID
	}
	return res
}

// --- amount helpers ---

// parseAmount converts a decimal string ("10.5000") to a fixed-point int64.
// Uses string parsing to avoid any floating-point rounding.
func parseAmount(s string) (int64, error) {
	parts := strings.SplitN(strings.TrimSpace(s), ".", 2)
	intPart, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid integer part: %w", err)
	}

	var fracPart int64
	if len(parts) == 2 {
		frac := parts[1]
		for len(frac) < 4 {
			frac += "0"
		}
		frac = frac[:4]
		fracPart, err = strconv.ParseInt(frac, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid decimal part: %w", err)
		}
	}

	return intPart*domain.BalanceScale + fracPart, nil
}

// formatAmount converts a fixed-point int64 back to a decimal string.
func formatAmount(amount int64) string {
	return fmt.Sprintf("%d.%04d", amount/domain.BalanceScale, amount%domain.BalanceScale)
}

// --- helpers ---

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v) //nolint:errcheck
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
