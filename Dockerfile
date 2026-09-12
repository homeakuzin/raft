FROM golang:1.26 AS test
COPY . .
RUN CGO_ENABLED=1 go test . -c -race -o /raft.test
CMD ["/raft.test", "-test.v"]

FROM golang:1.26-alpine AS main
COPY . .
RUN go build -o /raft .
ENTRYPOINT ["/raft"]

