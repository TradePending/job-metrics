# syntax=docker/dockerfile:1
FROM golang:1.23.3-alpine AS builder
WORKDIR /app
COPY go.mod .
COPY go.sum .
COPY main.go .
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o sidekiq-metrics main.go
RUN chmod +x sidekiq-metrics

FROM alpine:3.19
WORKDIR /app
COPY --from=builder /app/sidekiq-metrics ./sidekiq-metrics
ENTRYPOINT ["/app/sidekiq-metrics"]
