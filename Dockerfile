FROM golang:1.26 AS base
WORKDIR /app
COPY go.mod go.sum .
RUN go mod download
COPY . .

FROM base AS test
RUN CGO_ENABLED=1 go test . -c -race -o /raft.test
ENTRYPOINT ["/raft.test", "-test.v"]

FROM base AS build
RUN CGO_ENABLED=0 go build \
    -ldflags "-X main.buildVersion=$(date -u +%Y%m%d%H%M%S)" \
    -o /raft .

FROM scratch AS main
COPY --from=build /raft /raft
ENTRYPOINT ["/raft"]
