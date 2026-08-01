FROM golang:1.26-alpine AS builder

WORKDIR /build
COPY go.mod go.sum .
RUN go mod download
COPY . .

RUN CGO_ENABLED=0 go build -o raft-log-http ./cmd/list

FROM scratch AS app
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /build/raft-log-http /raft-log-http
