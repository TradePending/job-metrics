# syntax=docker/dockerfile:1
FROM golang:1.25.14-alpine AS builder
WORKDIR /app
COPY go.mod .
COPY go.sum .
COPY main.go .
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o job-metrics main.go
RUN chmod +x job-metrics

FROM alpine:3.19
WORKDIR /app
COPY --from=builder /app/job-metrics ./job-metrics
ENTRYPOINT ["/app/job-metrics"]
