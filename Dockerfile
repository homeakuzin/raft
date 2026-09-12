FROM golang:1.26 AS base

COPY . .

FROM base AS test

RUN CGO_ENABLED=1 go test . -c -race -o /raft.test

CMD ["/raft.test", "-test.v"]