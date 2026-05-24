package main

import (
	"context"
	"flag"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"

	"github.com/homeakuzin/raft"
	"github.com/homeakuzin/raft/storage"
)

var flagNodes = flag.String("nodes", "", "Cluster configuration")
var flagKvNodes = flag.String("clientnodes", "", "Client servers")
var flagNodeId = flag.String("id", "", "Node id")
var flagAuthToken = flag.String("authtoken", "", "HTTP auth token for communicating between nodes")

func main() {
	flag.Parse()
	nodeIdInt, err := strconv.Atoi(*flagNodeId)
	if err != nil {
		slog.Error("invalid -id usage", "value", *flagNodeId, "err", err)
		os.Exit(1)
	}
	nodes, err := raft.ParseNodesFlag(*flagNodes)
	if err != nil {
		slog.Error("invalid -nodes usage", "value", *flagNodes, "err", err)
		os.Exit(1)
	}
	clientNodes, err := raft.ParseNodesFlag(*flagKvNodes)
	if err != nil {
		slog.Error("invalid -clientnodes usage", "value", *flagNodes, "err", err)
		os.Exit(1)
	}
	if *flagAuthToken == "" {
		slog.Error("please provide -authtoken")
		os.Exit(1)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	nodeId := raft.NodeId(nodeIdInt)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	list := &storage.ListStorage{}

	transport := raft.HTTPTransport(nodeId, nodes, logger, *flagAuthToken)
	node := raft.NewNode(nodeId, nodes, transport, list, logger)
	slog.Info("starting cluster node", "id", nodeId)
	nodeCh := make(chan struct{})
	go func() {
		if err := node.Run(ctx); err != nil {
			slog.Error("could not start node", "err", err)
			cancel()
			nodeCh <- struct{}{}
		}
	}()

	handler := http.NewServeMux()
	handler.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		slog.Info("GET request", "client", r.RemoteAddr, "userAgent", r.UserAgent())
		w.Write(list.Last())
	})
	handler.HandleFunc("POST /", func(w http.ResponseWriter, r *http.Request) {
		slog.Info("POST request", "client", r.RemoteAddr, "userAgent", r.UserAgent())
		// TODO move into raft package
		if node.State() != raft.Leader {
			nextId := raft.EmptyId
			for id := range clientNodes {
				if id != nodeId && r.Header.Get("X-Raft-Next-Node"+strconv.Itoa(int(id))) == "" {
					nextId = id
					break
				}
			}
			if nextId == raft.EmptyId {
				w.WriteHeader(500)
				w.Write([]byte("no more nodes to try"))
				slog.Info("no more nodes to try")
				return
			}
			slog.Info("not a leader. try next node", "next", nextId, "addr", clientNodes[nextId])
			req, err := http.NewRequest("POST", "http://"+clientNodes[nextId], r.Body)
			if err != nil {
				w.WriteHeader(500)
				slog.Error("could not build next node request", "err", err)
				return
			}
			req.Header = r.Header
			req.Header.Set("X-Raft-Next-Node"+strconv.Itoa(int(nextId)), "1")
			client := http.Client{}
			resp, err := client.Do(req)
			if err != nil {
				w.WriteHeader(500)
				slog.Error("failed next node request", "err", err)
				return
			}
			w.WriteHeader(resp.StatusCode)
			if _, err := io.Copy(w, resp.Body); err != nil {
				slog.Error("could not read next node body", "err", err)
				return
			}
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			slog.Error("could not read request body", "err", err)
			w.WriteHeader(500)
			return
		}
		node.ClientCommand(r.Context(), body)
	})
	addr := clientNodes[nodeId]
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		slog.Error("could not start list listener", "err", err)
		os.Exit(1)
	}
	listServer := &http.Server{Addr: addr, Handler: handler}
	listServerCh := make(chan struct{})
	slog.Info("starting list server", "addr", addr)
	go func() {
		if err := listServer.Serve(ln); err != nil {
			slog.Error("could not start list server", "err", err)
			cancel()
			listServerCh <- struct{}{}
		}
	}()
	<-listServerCh
	<-nodeCh
	os.Exit(1)
}
