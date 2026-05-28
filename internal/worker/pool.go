package worker

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/boeing/go-ledger-x/internal/domain"
	"github.com/boeing/go-ledger-x/internal/kafka"
	"github.com/boeing/go-ledger-x/internal/repository"
)

type JobType string

const (
	JobCredit   JobType = "credit"
	JobDebit    JobType = "debit"
	JobTransfer JobType = "transfer"
)

type Job struct {
	Type     JobType
	Credit   *domain.CreditRequest
	Debit    *domain.DebitRequest
	Transfer *domain.TransferRequest
	Result   chan<- Result
}

type Result struct {
	Transaction *domain.Transaction
	Err         error
}

type Pool struct {
	jobs     chan Job
	repo     *repository.WalletRepository
	producer *kafka.Producer
	size     int
	wg       sync.WaitGroup
}

func NewPool(size, bufferSize int, repo *repository.WalletRepository, producer *kafka.Producer) *Pool {
	return &Pool{
		jobs:     make(chan Job, bufferSize),
		repo:     repo,
		producer: producer,
		size:     size,
	}
}

func (p *Pool) Start(ctx context.Context) {
	for range p.size {
		p.wg.Add(1)
		go p.run(ctx)
	}
}

// Submit enqueues a job. Blocks if the buffer is full.
func (p *Pool) Submit(job Job) {
	p.jobs <- job
}

// Shutdown closes the job channel and waits for all workers to drain.
func (p *Pool) Shutdown() {
	close(p.jobs)
	p.wg.Wait()
}

func (p *Pool) run(ctx context.Context) {
	defer p.wg.Done()
	for job := range p.jobs {
		p.process(ctx, job)
	}
}

func (p *Pool) process(ctx context.Context, job Job) {
	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var res Result
	switch job.Type {
	case JobCredit:
		res.Transaction, res.Err = p.repo.Credit(tctx, *job.Credit)
	case JobDebit:
		res.Transaction, res.Err = p.repo.Debit(tctx, *job.Debit)
	case JobTransfer:
		res.Transaction, res.Err = p.repo.Transfer(tctx, *job.Transfer)
	}

	if res.Err == nil && res.Transaction != nil {
		event := kafka.TransactionEvent{
			EventType:      "transaction." + string(res.Transaction.Status),
			TransactionID:  res.Transaction.ID,
			IdempotencyKey: res.Transaction.IdempotencyKey,
			Type:           string(res.Transaction.Type),
			Amount:         res.Transaction.Amount,
			Status:         string(res.Transaction.Status),
		}
		if err := p.producer.PublishTransactionEvent(ctx, event); err != nil {
			slog.Error("publish transaction event", "transaction_id", event.TransactionID, "err", err)
		}
	}

	if job.Result != nil {
		job.Result <- res
	}
}
