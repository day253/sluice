# ---- Build stage ----
FROM golang:1.26-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /sluice ./cmd/sluice/ && \
    CGO_ENABLED=0 go build -ldflags="-s -w" -o /sluice-operator ./cmd/operator/ && \
    CGO_ENABLED=0 go build -ldflags="-s -w" -o /sluice-autoscaler ./cmd/autoscaler/

# ---- Runtime stage ----
FROM alpine:3.21

RUN apk add --no-cache ca-certificates

COPY --from=builder /sluice /usr/local/bin/sluice
COPY --from=builder /sluice-operator /usr/local/bin/sluice-operator
COPY --from=builder /sluice-autoscaler /usr/local/bin/sluice-autoscaler

EXPOSE 9090 7000

ENTRYPOINT ["/usr/local/bin/sluice"]
