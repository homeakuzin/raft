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
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/VictoriaMetrics/metrics"
)

const clientAddrsFlag = "clientaddrs"
const raftAddrsFlag = "raftaddrs"

var flagBenchDuration = flag.Duration("d", 0, "Benchmark duration (minimum 15s; 0 runs a node)")
var flagBenchConcurrent = flag.Int("c", 1, "Concurrent clients")
var flagBenchmarkID = flag.String("benchmarkid", "", "Benchmark result directory name (defaults to date and time)")
var flagCertFile = flag.String("certfile", "", "")
var flagKeyFile = flag.String("keyfile", "", "")

func main() {
	flag.Parse()
	if *flagBenchDuration != 0 && *flagBenchDuration < 15*time.Second {
		slog.Error("benchmark duration must be at least 15s", "duration", *flagBenchDuration)
		os.Exit(1)
	}
	clientAddrMap := parseAndValidateAddrs("RAFT_CLIENT_ADDRS")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	logLevel := slog.LevelInfo
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	})
	slog.SetDefault(slog.New(handler))

	if *flagBenchDuration > 0 {
		debugAddrMap := parseAndValidateAddrs("RAFT_DEBUG_ADDRS")
		err := runBenchmarks(ctx, clientAddrMap, debugAddrMap)
		if err != nil {
			slog.Error("benchmark error", "err", err.Error())
			os.Exit(1)
		}
		return
	}

	nodeId := NodeId(os.Getenv("RAFT_NODE_ID"))
	if nodeId == None {
		slog.Error("expected RAFT_NODE_ID")
		os.Exit(1)
	}

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
				_, password, basicAuthOK := r.BasicAuth()
				validTokenAuth := r.Header.Get("X-Auth-Token") == pprofAuth || r.URL.Query().Get("authToken") == pprofAuth
				if (!basicAuthOK || password != pprofAuth) && !validTokenAuth {
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
	// tr := NewHttpTransport(ln, nodeId, raftAddrMap, raftLogger)
	tr, err := NewHttp2Transport(ln, nodeId, raftAddrMap, raftLogger, *flagCertFile, *flagKeyFile)
	if err != nil {
		slog.Error("could not create http2 transport", "err", err)
		os.Exit(1)
	}
	node := NewNode(nodeId, otherIds(raftAddrMap, nodeId), raftLogger, tr)
	version, err := strconv.ParseInt(buildVersion, 10, 64)
	if err != nil {
		slog.Error("invalid build version", "version", buildVersion, "err", err)
		os.Exit(1)
	}
	metrics.GetOrCreateGauge("raft_version", func() float64 {
		return float64(version)
	})
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

func runBenchmarks(ctx context.Context, clientAddrMap, debugAddrMap map[NodeId]string) error {
	benchmarkStart := time.Now()
	resultDir, err := createBenchmarkResultDir(benchmarkStart)
	if err != nil {
		return fmt.Errorf("create benchmark result directory: %w", err)
	}
	fmt.Printf("Start benchmark testing %s\n", benchmarkStart.Format(time.DateTime))
	fmt.Printf("Results will be saved to %s\n", resultDir)
	ctx, cancel := context.WithTimeout(ctx, *flagBenchDuration)
	defer cancel()
	profilesDone := make(chan struct{})
	go func() {
		defer close(profilesDone)
		collectProfiles(ctx, resultDir, debugAddrMap)
	}()
	defer func() {
		cancel()
		<-profilesDone
	}()
	clientsSemaphore := make(chan struct{}, *flagBenchConcurrent)
	leader, err := discoverLeader(ctx, clientAddrMap)
	if err != nil {
		return fmt.Errorf("could not discover leader: %w", err)
	}
	resultMu := &sync.Mutex{}
	requestsWg := &sync.WaitGroup{}
	counter := 0
	latency := make([]time.Duration, 0, *flagBenchConcurrent)
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case clientsSemaphore <- struct{}{}:
		}

		requestsWg.Add(1)
		go func() {
			defer requestsWg.Done()
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
				if ctx.Err() != nil {
					return
				}
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
	requestsWg.Wait()
	slices.Sort(latency)
	if err := writeBenchmarkResult(resultDir, benchmarkStart, counter); err != nil {
		return fmt.Errorf("write benchmark result: %w", err)
	}
	fmt.Printf("Total requests: %d\n", counter)
	fmt.Printf("Concurrent clients: %d\n", *flagBenchConcurrent)
	fmt.Printf("Time elapsed: %s\n", time.Since(benchmarkStart).String())
	if len(latency) > 0 {
		fmt.Printf("Latency:\n")
		fmt.Printf("\tp50: %s\n", latency[percentileIndex(len(latency), 50)])
		fmt.Printf("\tp90: %s\n", latency[percentileIndex(len(latency), 90)])
		fmt.Printf("\tp99: %s\n", latency[percentileIndex(len(latency), 99)])
	}
	return nil
}

func createBenchmarkResultDir(start time.Time) (string, error) {
	name := start.Format("2006-01-02_15-04-05")
	if *flagBenchmarkID != "" {
		name = *flagBenchmarkID
		if name == "." || name == ".." || strings.ContainsAny(name, `/\`) {
			return "", errors.New("benchmarkid must be a directory name, not a path")
		}
	}
	resultDir := filepath.Join("raft_benchmarks", name)
	return resultDir, os.MkdirAll(resultDir, 0o755)
}

func writeBenchmarkResult(resultDir string, start time.Time, completedRequests int) error {
	result := fmt.Sprintf(
		"benchmark_id: %s\nstarted_at: %s\nduration: %s\nconcurrent_clients: %d\ncompleted_requests: %d\n",
		filepath.Base(resultDir), start.Format(time.RFC3339), *flagBenchDuration, *flagBenchConcurrent, completedRequests,
	)
	filename := fmt.Sprintf("%s_%s_%d.txt", start.Format("2006-01-02_15-04-05"), *flagBenchDuration, *flagBenchConcurrent)
	return os.WriteFile(filepath.Join(resultDir, filename), []byte(result), 0o644)
}

func percentileIndex(length, percentile int) int {
	index := length * percentile / 100
	if index == length {
		return length - 1
	}
	return index
}

func collectProfiles(ctx context.Context, resultDir string, debugAddrMap map[NodeId]string) {
	var collector benchmarkProfileCollector
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case capturedAt := <-ticker.C:
			var profiles sync.WaitGroup
			for nodeID, debugAddr := range debugAddrMap {
				for _, profileType := range []string{"profile", "allocs", "heap", "goroutine"} {
					profiles.Add(1)
					go func() {
						defer profiles.Done()
						collector.collectNodeProfile(ctx, resultDir, nodeID, debugAddr, profileType, capturedAt)
					}()
				}
			}
			profiles.Wait()
		}
	}
}

type benchmarkProfileCollector struct {
	cpuLocks sync.Map // debug address -> *sync.Mutex; shared even when node IDs differ.
}

func (c *benchmarkProfileCollector) collectNodeProfile(ctx context.Context, resultDir string, nodeID NodeId, debugAddr, profileType string, capturedAt time.Time) {
	if ctx.Err() != nil {
		return
	}
	cpuComplete := false
	if profileType == "profile" {
		value, _ := c.cpuLocks.LoadOrStore(debugAddr, &sync.Mutex{})
		lock := value.(*sync.Mutex)
		if !lock.TryLock() {
			slog.Warn("skip CPU profile: previous collection is pending or unconfirmed", "node_id", nodeID)
			return
		}
		// A failed download does not prove the remote profiler has stopped.
		// Keep this address locked for the rest of the run unless it completes.
		defer func() {
			if cpuComplete {
				lock.Unlock()
			} else if ctx.Err() == nil {
				slog.Warn("CPU profiling disabled for this run: completion not confirmed", "node_id", nodeID)
			}
		}()
	}
	requestCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	profileURL := "http://" + debugAddr + "/debug/pprof/" + profileType
	if profileType == "profile" {
		profileURL += "?seconds=5"
	}
	req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, profileURL, nil)
	if err != nil {
		slog.Error("create profile request", "node_id", nodeID, "profile", profileType, "err", err)
		return
	}
	req.SetBasicAuth("raft", os.Getenv("PPROF_AUTH"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		slog.Error("collect profile", "node_id", nodeID, "profile", profileType, "err", err)
		return
	}
	body, readErr := io.ReadAll(resp.Body)
	resp.Body.Close()
	if readErr != nil {
		if ctx.Err() != nil {
			return
		}
		slog.Error("read profile", "node_id", nodeID, "profile", profileType, "err", readErr)
		return
	}
	if resp.StatusCode != http.StatusOK {
		slog.Error("collect profile", "node_id", nodeID, "profile", profileType, "status", resp.Status)
		return
	}
	cpuComplete = true
	nodeDir := filepath.Join(resultDir, nodeID.String())
	if err := os.MkdirAll(nodeDir, 0o755); err != nil {
		slog.Error("create profile directory", "node_id", nodeID, "err", err)
		return
	}
	filename := profileType + "_" + capturedAt.Format("15-04-05") + ".prof"
	if err := os.WriteFile(filepath.Join(nodeDir, filename), body, 0o644); err != nil {
		slog.Error("save profile", "node_id", nodeID, "profile", profileType, "err", err)
	}
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
				slog.Error("node GET state error", "err", ns.err)
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
