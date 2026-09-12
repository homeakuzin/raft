package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
)

const clientAddrsFlag = "clientaddrs"
const raftAddrsFlag = "raftaddrs"

var flagNodeId = flag.String("nodeid", "", "Node ID")
var flagClientAddrs = flag.String(clientAddrsFlag, "", "")
var flagRaftAddr = flag.String(raftAddrsFlag, "", "")

func main() {
	flag.Parse()

	logLevel := slog.LevelInfo
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	})
	slog.SetDefault(slog.New(handler))

	nodeId := NodeId(*flagNodeId)
	raftAddrMap := parseAndValidateAddrsFlag(flagRaftAddr, raftAddrsFlag, nodeId)

	// clientAddrMap := parseAndValidateAddrsFlag(flagClientAddrs, clientAddrsFlag, nodeId)
	// _ = clientAddrMap

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	ln, err := net.Listen("tcp", raftAddrMap[nodeId])
	if err != nil {
		slog.Error("could not start raft listener", "err", err)
		os.Exit(1)
	}
	raftLogger := NewRaftLogger(slog.Default())
	tr := NewHttpTransport(ln, nodeId, raftAddrMap, raftLogger)
	node := NewNode(nodeId, otherIds(raftAddrMap, nodeId), raftLogger, tr)
	node.Run(ctx)
}

func otherIds(addrs map[NodeId]string, nodeId NodeId) []NodeId {
	ids := make([]NodeId, 0, len(addrs))
	for id := range addrs {
		if id != nodeId {
			ids = append(ids, id)
		}
	}
	return ids
}

func parseAndValidateAddrsFlag(flagValue *string, flagName string, nodeId NodeId) map[NodeId]string {
	if *flagValue == "" {
		slog.Error("addrs flag required", "flag", flagName)
		os.Exit(1)
	}
	result, err := parseAddrsFlag(*flagValue)
	if err != nil {
		slog.Error("invalid addrs flag value", "err", err, "flag", flagName)
		os.Exit(1)
	}
	if len(result) != 3 {
		slog.Error("invalid addrs flag value", "err", "expected exactly 3 nodes", "actual", len(result), "flag", flagName)
		os.Exit(1)
	}
	if _, ok := result[nodeId]; !ok {
		slog.Error("invalid addrs flag value", "err", "addr not provided for current node")
		os.Exit(1)
	}
	return result
}

func parseAddrsFlag(nodesStr string) (map[NodeId]string, error) {
	nodes := make(map[NodeId]string, 3)
	nodeParts := strings.Split(nodesStr, ",")
	for i := range nodeParts {
		idHostAndPort := strings.Split(nodeParts[i], ":")
		if len(idHostAndPort) != 3 {
			return nil, fmt.Errorf("invalid peer configuration: %s", nodeParts[i])
		}
		nodes[NodeId(idHostAndPort[0])] = idHostAndPort[1] + ":" + idHostAndPort[2]
	}

	return nodes, nil
}
