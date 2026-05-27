# Technical Assessment: High-Performance Concurrent Financial Ledger Engine (LedgerX)

## Context & System Overview

Your team is building **LedgerX**, a high-throughput financial ecosystem responsible for managing digital wallets and processing real-time fund transfers. In production, this system is deployed in a microservices environment behind a load balancer and faces high traffic spikes, meaning multiple instances of the service process transactions simultaneously.

Because LedgerX handles actual financial data, it cannot afford any data corruption, missing balances, or race conditions. If an user clicks "Pay" multiple times due to a laggy internet connection, or if multiple transactions target the same account at the exact same millisecond, the system must handle it gracefully, accurately, and deterministically.

---

## Your Task

Act as the Senior Backend Engineer. Your mission is to design and implement a production-ready, highly concurrent transaction processing backend service in **Go (Golang)** that safely modifies wallet balances and logs execution results.

### Core Architectural Requirements

#### 1. Database Schema & Relational Integrity

Set up a relational database (PostgreSQL or MySQL preferred). You must design and provide a SQL schema migration file that sets up the following entities:

* **Wallets:** Tracks user accounts, balances (using accurate financial data types, not floating points), and update metadata.
* **Transactions:** Tracks historical fund movements. Every transaction record must enforce a globally unique identifier (UUID) and track an `idempotency_key` to avoid duplicate billing.

#### 2. Advanced Concurrency Control (No Race Conditions)

You must protect the system against **data race anomalies** and multi-allocation bugs.

* If two worker processes attempt to deduct funds from or credit funds to the exact same wallet row concurrently, your system must use **SQL Row-Level Locking** (`FOR UPDATE`) wrapped inside an isolated database transaction block.
* Your engine must guarantee that an account balance can never drop below zero, even under heavy concurrent load.

#### 3. High-Throughput Event-Driven Architecture (Worker Pool & Channels)

To prevent incoming HTTP/gRPC network threads from bottlenecking or blocking during heavy database latency, you must decouple request ingestion from processing.

* Incoming HTTP transfer requests must be quickly parsed, validated, and pushed onto a buffered **Go Channel**.
* Implement a **Worker Pool Pattern** consisting of a fixed, configurable number of background goroutines that continuously pull payloads off the channel and execute the database operations concurrently.

#### 4. System Resiliency & Resource Management

Your backend code must be defensive and prevent resource/goroutine leaks:

* **Context Propagation:** Every database query must respect a defined context timeout. If a query hangs or a deadlock occurs, the context must abort the operation within 3 to 5 seconds.
* **Graceful Shutdown:** If the server receives an operating system termination signal (like `SIGINT` or `SIGTERM`), it must stop accepting new HTTP requests, allow the background worker pool to safely finish processing any remaining transactions floating in the channel buffer, commit active database transactions, and close connection pools cleanly before exiting the binary.

#### 5. Strict Idempotency Implementation

Before executing any state change or deducting a balance, the ledger engine must verify the `idempotency_key` passed in the request. If a key has already been processed or is currently in flight, the engine must reject the duplicate execution attempt safely without altering balances a second time.

---

## Submission & Evaluation Criteria

The grading team will evaluate your code based on the following metrics:

1. **Idiomatic Go Code:** Proper project directory layouts (e.g., separating `/cmd` from your `/internal` business layers), explicit error handling, and avoiding global state variables.
2. **Concurrency Testing:** You must provide a table-driven concurrency test (`go test`) using a framework like `sync.WaitGroup` that fires 100 simultaneous, rapid-fire mock requests against a single wallet row. The test must prove that balances remain perfectly balanced and zero race conditions occur.
3. **Portability:** Provide a simple `docker-compose.yml` or shell setup script so the reviewer can run your entire stack (the Go service and the database instance) with a single command.