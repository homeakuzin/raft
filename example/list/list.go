package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"net/http"
	"os"
	"sync"

	"github.com/homeakuzin/raft"
	"github.com/homeakuzin/raft/storage"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var flagRaftListen = flag.String("raftlisten", "", "Listen raft connections at")
var flagClientListen = flag.String("clientlisten", "", "Listen client connections at")
var flagNodes = flag.String("nodes", "", "Cluster configuration")
var flagKvNodes = flag.String("clientnodes", "", "Client servers")
var flagNodeId = flag.String("id", "", "Node id")
var flagAuthToken = flag.String("authtoken", "", "HTTP auth token for communicating between nodes")
var flagLoadNodes = flag.String("loadnodes", "", "Run load command")
var flagLoadParams = flag.String("loadparams", "", "Load params (see `type loadParams struct`)")

func main() {
	flag.Parse()
	if *flagLoadNodes != "" {
		runLoad()
	} else {
		runNode()
	}
}

func runLoad() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	nodes, err := raft.ParseNodesFlag(*flagLoadNodes)
	if err != nil {
		logger.Error("invalid -loadnodes usage", "value", *flagNodes, "err", err)
		os.Exit(1)
	}

	var params loadParams
	if err := json.Unmarshal([]byte(*flagLoadParams), &params); err != nil {
		logger.Error("invalid -loadparams", "err", err)
		os.Exit(1)
	}
	logger.Info("run load testing", "params", params)
	wg := &sync.WaitGroup{}
	for id, node := range nodes {
		wg.Add(1)
		go loadNode(logger, wg, id, node, params)
	}
	wg.Wait()
	logger.Info("done load testing", "params", params)
}

type loadParams struct {
	LimitLogs int     `json:"limit"`
	RWRate    float64 `json:"rw"`
	Parallel  int     `json:"p"`
	BodySize  int     `json:"sz"`
}

func loadNode(logger *slog.Logger, wg *sync.WaitGroup, id raft.NodeId, addr string, params loadParams) {
	logger.Info("loading node", "id", id, "addr", addr)
	logsSent := 0
	sema := make(chan struct{}, params.Parallel)
	for logsSent < params.LimitLogs {
		client := http.Client{}
		decision := rand.Float64()
		readChance := 1 / (1 + params.RWRate) * params.RWRate
		var req *http.Request
		if decision < readChance {
			req, _ = http.NewRequest("GET", "http://"+addr, nil)
		} else {
			logsSent++
			body := make([]byte, params.BodySize)
			for i := range len(body) {
				body[i] = byte(rand.Intn(256))
			}
			req, _ = http.NewRequest("POST", "http://"+addr, bytes.NewBuffer([]byte(body)))
		}
		req.Close = true
		sema <- struct{}{}
		go func() {
			resp, err := client.Do(req)
			if err == nil {
				resp.Body.Close()
				if resp.StatusCode != 200 {
					logger.Info("node returned non-200 status code", "node", id, "statusCode", resp.StatusCode)
				}
			}
			<-sema
		}()
	}
	logger.Info("done loading node", "node", id)
	wg.Done()
}

func runNode() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	if *flagNodeId == "" {
		logger.Error("provide -id")
		os.Exit(1)
	}
	nodes, err := raft.ParseNodesFlag(*flagNodes)
	if err != nil {
		logger.Error("invalid -nodes usage", "value", *flagNodes, "err", err)
		os.Exit(1)
	}
	clientNodes, err := raft.ParseNodesFlag(*flagKvNodes)
	if err != nil {
		logger.Error("invalid -clientnodes usage", "value", *flagNodes, "err", err)
		os.Exit(1)
	}
	if *flagAuthToken == "" {
		logger.Error("please provide -authtoken")
		os.Exit(1)
	}

	nodeId := raft.NodeId(*flagNodeId)
	logger = logger.With("node", nodeId)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	list := &storage.ListStorage{}

	transport := raft.HTTPTransport(nodeId, *flagRaftListen, nodes, logger, *flagAuthToken)
	node := raft.NewNode(nodeId, nodes, transport, list, logger)
	hostname, err := os.Hostname()
	if err != nil {
		slog.Error("could not get hostname", "err", err)
	}
	logger.Info("starting cluster node", "id", nodeId, "hostname", hostname)
	nodeCh := make(chan struct{})
	go func() {
		if err := node.Run(ctx); err != nil {
			logger.Error("could not start node", "err", err)
			cancel()
			nodeCh <- struct{}{}
		}
	}()

	handler := http.NewServeMux()
	handler.Handle("GET /metrics", promhttp.Handler())
	handler.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		logger.Info("GET request", "client", r.RemoteAddr, "userAgent", r.UserAgent())
		w.Write(list.Last())
	})
	handler.HandleFunc("POST /", func(w http.ResponseWriter, r *http.Request) {
		logger.Info("POST request", "client", r.RemoteAddr, "userAgent", r.UserAgent())
		// TODO move into raft package
		if node.State() != raft.Leader {
			nextId := raft.EmptyId
			for id := range clientNodes {
				if id != nodeId && r.Header.Get("X-Raft-Next-Node-"+string(id)) == "" {
					nextId = id
					break
				}
			}
			if nextId == raft.EmptyId {
				w.WriteHeader(500)
				w.Write([]byte("no more nodes to try"))
				logger.Info("no more nodes to try")
				return
			}
			logger.Info("not a leader. try next node", "next", nextId, "addr", clientNodes[nextId])
			req, err := http.NewRequest("POST", "http://"+clientNodes[nextId], r.Body)
			if err != nil {
				w.WriteHeader(500)
				logger.Error("could not build next node request", "err", err)
				return
			}
			req.Header = r.Header
			req.Header.Set("X-Raft-Next-Node"+string(nextId), "1")
			client := http.Client{}
			resp, err := client.Do(req)
			if err != nil {
				w.WriteHeader(500)
				logger.Error("failed next node request", "err", err)
				return
			}
			w.WriteHeader(resp.StatusCode)
			if _, err := io.Copy(w, resp.Body); err != nil {
				logger.Error("could not read next node body", "err", err)
				return
			}
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			logger.Error("could not read request body", "err", err)
			w.WriteHeader(500)
			return
		}
		node.ClientCommand(r.Context(), body)
	})
	addr := clientNodes[nodeId]
	ln, err := net.Listen("tcp", *flagClientListen)
	if err != nil {
		logger.Error("could not start list listener", "err", err)
		os.Exit(1)
	}
	listServer := &http.Server{Addr: addr, Handler: handler}
	listServerCh := make(chan struct{})
	logger.Info("starting list server", "addr", addr)
	go func() {
		if err := listServer.Serve(ln); err != nil {
			logger.Error("could not start list server", "err", err)
			cancel()
			listServerCh <- struct{}{}
		}
	}()
	<-listServerCh
	<-nodeCh
	os.Exit(1)
}
