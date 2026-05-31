FROM golang:1.26-alpine AS builder

WORKDIR /build
COPY go.mod go.sum .
RUN go mod download
COPY . .

RUN CGO_ENABLED=0 go build -o raft-log-http

FROM scratch AS app
COPY --from=builder /build/raft-log-http /raft-log-http