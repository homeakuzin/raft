package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/VictoriaMetrics/metrics"
)

const clientAddrsFlag = "clientaddrs"
const raftAddrsFlag = "raftaddrs"

var flagBenchDuration = flag.Duration("d", 0, "Benchmark duration")
var flagBenchConcurrent = flag.Int("c", 1, "Concurrent clients")

func main() {
	flag.Parse()
	clientAddrMap := parseAndValidateAddrs("RAFT_CLIENT_ADDRS")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	logLevel := slog.LevelInfo
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	})
	slog.SetDefault(slog.New(handler))

	if *flagBenchDuration > 0 {
		err := runBenchmarks(ctx, clientAddrMap)
		if err != nil {
			slog.Error("benchmark error", "err", err.Error())
			os.Exit(1)
		}
		return
	}

	nodeId := NodeId(os.Getenv("RAFT_NODE_ID"))

	slog.SetDefault(slog.New(handler).With("node_id", nodeId))

	raftAddrMap := parseAndValidateAddrs("RAFT_ADDRS")
	if _, ok := raftAddrMap[nodeId]; !ok {
		slog.Error("invalid addrs value", "err", "addr not provided for current node")
		os.Exit(1)
	}
	var clientAddr string
	var ok bool
	if clientAddr, ok = clientAddrMap[nodeId]; !ok {
		slog.Error("invalid addrs value", "err", "addr not provided for current node")
		os.Exit(1)
	}

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
	debugLevel := os.Getenv("RAFT_DEBUG")
	switch debugLevel {
	case "1":
		raftLogger.DebugLevel(1)
	case "2":
		raftLogger.DebugLevel(2)
	case "3":
		raftLogger.DebugLevel(3)
	}
	if debugLevel != "" {
		slog.Info("debug level", "level", debugLevel)
	}
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

func runBenchmarks(ctx context.Context, clientAddrMap map[NodeId]string) error {
	ctx, cancel := context.WithTimeout(ctx, *flagBenchDuration)
	defer cancel()
	clientsSemaphore := make(chan struct{}, *flagBenchConcurrent)
	leader, err := discoverLeader(ctx, clientAddrMap)
	if err != nil {
		return fmt.Errorf("could not discover leader: %w", err)
	}
	resultMu := &sync.Mutex{}
	counter := 0
	latency := make([]time.Duration, 0, *flagBenchConcurrent)
	benchmarkStart := time.Now()
	// leaderMu := &sync.Mutex{}
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case clientsSemaphore <- struct{}{}:
		}

		go func() {
			defer func() { <-clientsSemaphore }()
			data := command(16)
			req, err := http.NewRequestWithContext(ctx, "GET", "http://"+clientAddrMap[leader]+"/"+string(data), nil)
			if err != nil {
				slog.Error("client error", "err", err)
				cancel()
				return
			}
			start := time.Now()
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				slog.Error("client error", "err", err)
				cancel()
				return
			}
			defer resp.Body.Close()
			resultMu.Lock()
			counter++
			latency = append(latency, time.Since(start))
			resultMu.Unlock()
		}()
	}
	slices.Sort(latency)
	p50Idx := int(float64(len(latency)) * (50 / 100.0))
	p90Idx := int(float64(len(latency)) * (90 / 100.0))
	p99Idx := int(float64(len(latency)) * (99 / 100.0))
	fmt.Printf("Total requests: %d\n", counter)
	fmt.Printf("Concurrent clients: %d\n", *flagBenchConcurrent)
	fmt.Printf("Time elapsed: %s\n", time.Since(benchmarkStart).String())
	fmt.Printf("Latency:\n")
	fmt.Printf("\tp50: %s (%d)\n", latency[p50Idx].String(), p50Idx)
	fmt.Printf("\tp90: %s (%d)\n", latency[p90Idx].String(), p90Idx)
	fmt.Printf("\tp99: %s (%d)\n", latency[p99Idx].String(), p99Idx)
	return nil
}

func command(n int) []byte {
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	b := make([]byte, n)
	for i := range b {
		for {
			var one [1]byte
			if _, err := rand.Read(one[:]); err != nil {
				panic(err)
			}

			if one[0] < 248 { // 62 * 4: без смещения распределения
				b[i] = alphabet[int(one[0])%len(alphabet)]
				break
			}
		}
	}

	return b
}

type nodeState struct {
	id    NodeId
	state State
	err   error
}

func discoverLeader(ctx context.Context, clientAddrMap map[NodeId]string) (NodeId, error) {
	leaderDiscoveryCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	for attempt := 0; attempt < 5; attempt++ {
		wg := &sync.WaitGroup{}
		states := make(chan nodeState, len(clientAddrMap))
		for id, addr := range clientAddrMap {
			wg.Add(1)
			go func() {
				defer wg.Done()
				req, _ := http.NewRequestWithContext(leaderDiscoveryCtx, "GET", "http://"+addr, nil)
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					states <- nodeState{err: fmt.Errorf("%s: %w", id.String(), err)}
					return
				}
				defer resp.Body.Close()
				state, err := io.ReadAll(resp.Body)
				if err != nil {
					states <- nodeState{err: fmt.Errorf("%s: %w", id.String(), err)}
					return
				}
				states <- nodeState{id: id, state: State(state)}
			}()
		}

		wg.Wait()
		close(states)
		for ns := range states {
			if ns.err != nil {
				return None, ns.err
			}
			if ns.state == Leader {
				return ns.id, nil
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return None, errors.New("leader has not been chosen for way too long")
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

func parseAndValidateAddrs(envName string) map[NodeId]string {
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
