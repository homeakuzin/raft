package main

import (
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/homeakuzin/raft"
	"github.com/homeakuzin/raft/storage"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func newListHandler(logger *slog.Logger, node *raft.Node, nodeID raft.NodeId, clientNodes map[raft.NodeId]string, list *storage.ListStorage, metrics *raftMetrics) http.Handler {
	handler := http.NewServeMux()
	handler.Handle("GET /metrics", promhttp.Handler())
	handler.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		defer metrics.observeRead(start)

		logger.Info("GET request", "client", r.RemoteAddr, "userAgent", r.UserAgent())
		_, _ = w.Write(list.Last())
	})
	handler.HandleFunc("POST /", func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		defer metrics.observeWrite(start)

		logger.Info("POST request", "client", r.RemoteAddr, "userAgent", r.UserAgent())
		if node.State() != raft.Leader {
			nextID := raft.EmptyId
			for id := range clientNodes {
				if id != nodeID && r.Header.Get("X-Raft-Next-Node-"+string(id)) == "" {
					nextID = id
					break
				}
			}
			if nextID == raft.EmptyId {
				w.WriteHeader(500)
				_, _ = w.Write([]byte("no more nodes to try"))
				logger.Info("no more nodes to try")
				return
			}
			logger.Info("not a leader. try next node", "next", nextID, "addr", clientNodes[nextID])
			req, err := http.NewRequest("POST", "http://"+clientNodes[nextID], r.Body)
			if err != nil {
				w.WriteHeader(500)
				logger.Error("could not build next node request", "err", err)
				return
			}
			req.Header = r.Header
			req.Header.Set("X-Raft-Next-Node"+string(nextID), "1")
			client := http.Client{}
			resp, err := client.Do(req)
			if err != nil {
				w.WriteHeader(500)
				logger.Error("failed next node request", "err", err)
				return
			}
			defer resp.Body.Close()
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
		replicateStart := time.Now()
		node.ClientCommand(r.Context(), body)
		metrics.observeReplicate(replicateStart)
	})
	return handler
}
