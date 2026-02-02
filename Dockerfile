# Build stage
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache gcc musl-dev

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build binaries
RUN CGO_ENABLED=1 GOOS=linux go build -o /bin/coordinator ./cmd/coordinator
RUN CGO_ENABLED=1 GOOS=linux go build -o /bin/coordinator-standalone ./cmd/coordinator-standalone
RUN CGO_ENABLED=1 GOOS=linux go build -o /bin/search ./cmd/search

# Runtime stage
FROM alpine:3.19

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

# Copy binaries from builder
COPY --from=builder /bin/coordinator /app/coordinator
COPY --from=builder /bin/coordinator-standalone /app/coordinator-standalone
COPY --from=builder /bin/search /app/search

# Create data directory
RUN mkdir -p /app/data

EXPOSE 8080 9090 9091

# Default to coordinator-standalone
CMD ["/app/coordinator-standalone"]
