package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"strings"

	"github.com/VictoriaMetrics/metrics"
)

const clientAddrsFlag = "clientaddrs"
const raftAddrsFlag = "raftaddrs"

func main() {
	nodeId := NodeId(os.Getenv("RAFT_NODE_ID"))

	logLevel := slog.LevelInfo
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	})
	slog.SetDefault(slog.New(handler).With("node_id", nodeId))

	raftAddrMap := parseAndValidateAddrs("RAFT_ADDRS", nodeId)
	clientAddr := parseAndValidateAddrs("RAFT_CLIENT_ADDRS", nodeId)[nodeId]

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	prometheusAddr := os.Getenv("PROMETHEUS_METRICS_ADDR")
	if prometheusAddr != "" {
		go func() {
			mux := http.NewServeMux()
			mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
				metrics.WritePrometheus(w, true)
			})
			slog.Info("metrics server listening", "addr", prometheusAddr)
			err := http.ListenAndServe(prometheusAddr, mux)
			if err != nil {
				slog.Error("metrics server error", "err", err)
				return
			}
		}()
	}

	otelExportAddr := os.Getenv("OTEL_EXPORT_ADDR")
	if otelExportAddr != "" {
		serviceName := "raft"
		slog.Info("exporting opentelemetry traces", "addr", otelExportAddr, "service_name", serviceName)
		ctx := context.Background()
		traceProvider, err := InitTracer(ctx, otelExportAddr, serviceName, true)
		if err != nil {
			slog.Error("could not start tracer", "error", err.Error())
		}
		defer traceProvider.Shutdown(ctx)
	}

	pprofAddr := os.Getenv("PPROF_ADDR")
	pprofAuth := os.Getenv("PPROF_AUTH")
	if pprofAddr != "" && pprofAuth != "" {
		go func() {
			var handler http.HandlerFunc = func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("X-Auth-Token") != pprofAuth && r.URL.Query().Get("authToken") != pprofAuth {
					w.WriteHeader(401)
					return
				}
				if r.URL.Path == "/debug/pprof/heap" {
					pprof.Handler("heap").ServeHTTP(w, r)
					return
				} else if r.URL.Path == "/debug/pprof/allocs" {
					pprof.Handler("allocs").ServeHTTP(w, r)
					return
				} else if r.URL.Path == "/debug/pprof/profile" {
					pprof.Profile(w, r)
					return
				} else if r.URL.Path == "/debug/pprof/goroutine" {
					pprof.Handler("goroutine").ServeHTTP(w, r)
					return
				}
			}

			slog.Info("pprof server listening", "addr", pprofAddr)
			err := http.ListenAndServe(pprofAddr, handler)
			if err != nil {
				slog.Error("pprof server error", "err", err)
				return
			}
		}()
	}

	clientLn, err := net.Listen("tcp", clientAddr)
	if err != nil {
		slog.Error("could not start client listener", "err", err)
		os.Exit(1)
	}

	ln, err := net.Listen("tcp", raftAddrMap[nodeId])
	if err != nil {
		slog.Error("could not start raft listener", "err", err)
		os.Exit(1)
	}
	raftLogger := NewRaftLogger(slog.Default())
	tr := NewHttpTransport(ln, nodeId, raftAddrMap, raftLogger)
	node := NewNode(nodeId, otherIds(raftAddrMap, nodeId), raftLogger, tr)
	metrics.GetOrCreateGauge("raft_state", func() float64 {
		return node.State().Float64()
	})
	metrics.GetOrCreateGauge("raft_term", func() float64 {
		return float64(node.CurrentTerm())
	})
	metrics.GetOrCreateGauge("raft_commit_index", func() float64 {
		return float64(node.CommitIndex())
	})
	startClientListener(clientLn, node)
	slog.Info("client server listening", "addr", clientAddr)
	node.Run(ctx)
}

const notALeaderResponse = "not a leader"

func startClientListener(ln net.Listener, node *Node) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(node.State()))
	})
	mux.HandleFunc("/{data}", func(w http.ResponseWriter, r *http.Request) {
		if node.State() != Leader {
			w.WriteHeader(500)
			w.Write([]byte(notALeaderResponse))
			return
		}
		ctx, span := Tracer.Start(r.Context(), "Client Command")
		data := r.PathValue("data")
		err := node.ClientCommand(ctx, []byte(data))
		if err != nil {
			w.WriteHeader(500)
			w.Write([]byte(err.Error()))
			EndSpanWithError(span, err)
			return
		}
		span.End()
		w.Write([]byte("OK"))
	})
	server := &http.Server{Handler: mux}
	go server.Serve(ln)
	return server
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

func parseAndValidateAddrs(envName string, nodeId NodeId) map[NodeId]string {
	value := os.Getenv(envName)
	if value == "" {
		slog.Error("addrs required", "name", envName)
		os.Exit(1)
	}
	result, err := parseAddrsFlag(value)
	if err != nil {
		slog.Error("invalid addrs value", "value", value, "err", err, "name", envName)
		os.Exit(1)
	}
	if len(result) != 3 {
		slog.Error("invalid addrs value", "value", value, "err", "expected exactly 3 nodes", "actual", len(result), "name", envName)
		os.Exit(1)
	}
	if _, ok := result[nodeId]; !ok {
		slog.Error("invalid addrs value", "value", value, "err", "addr not provided for current node")
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
