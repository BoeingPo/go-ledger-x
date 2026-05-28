FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /ledger-service ./cmd/server

FROM gcr.io/distroless/static-debian12
COPY --from=builder /ledger-service /ledger-service
EXPOSE 8081
ENTRYPOINT ["/ledger-service"]
