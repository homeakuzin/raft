package main

import (
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/homeakuzin/raft"
	"github.com/homeakuzin/raft/storage"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	raftotel "github.com/homeakuzin/raft/pkg/otel"
)

func newListHandler(logger *slog.Logger, node *raft.Node, nodeID raft.NodeId, clientNodes map[raft.NodeId]string, list *storage.ListStorage, metrics *raftMetrics) http.Handler {
	handler := http.NewServeMux()
	handler.Handle("GET /metrics", promhttp.Handler())
	handler.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		ctx, span := otel.Tracer("raft").Start(ctx, "list-get", trace.WithSpanKind(trace.SpanKindServer))
		defer span.End()

		start := time.Now()
		defer metrics.observeRead(start)

		logger.InfoContext(ctx, "GET request", "client", r.RemoteAddr, "userAgent", r.UserAgent())
		_, _ = w.Write(list.Last())
	})
	handler.HandleFunc("POST /", func(w http.ResponseWriter, r *http.Request) {
		ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		ctx, span := otel.Tracer("raft").Start(ctx, "Raft New Entry", trace.WithSpanKind(trace.SpanKindServer))
		w.Header().Add("X-Trace-Id", span.SpanContext().TraceID().String())
		w.Header().Add("X-Span-Id", span.SpanContext().SpanID().String())
		defer span.End()

		start := time.Now()
		defer metrics.observeWrite(start)

		logger.InfoContext(ctx, "POST request", "client", r.RemoteAddr, "userAgent", r.UserAgent())
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
				logger.InfoContext(ctx, "no more nodes to try")
				return
			}
			logger.InfoContext(ctx, "not a leader. try next node", "next", nextID, "addr", clientNodes[nextID])
			req, err := http.NewRequestWithContext(ctx, "POST", "http://"+clientNodes[nextID], r.Body)
			if err != nil {
				w.WriteHeader(500)
				raftotel.ErrSpan(span, err)
				logger.ErrorContext(ctx, "could not build next node request", "err", err)
				return
			}
			req.Header = r.Header.Clone()
			otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
			req.Header.Set("X-Raft-Next-Node-"+string(nextID), "1")
			client := http.Client{}
			resp, err := client.Do(req)
			if err != nil {
				w.WriteHeader(500)
				raftotel.ErrSpan(span, err)
				logger.ErrorContext(ctx, "failed next node request", "err", err)
				return
			}
			defer resp.Body.Close()
			w.WriteHeader(resp.StatusCode)
			if _, err := io.Copy(w, resp.Body); err != nil {
				raftotel.ErrSpan(span, err)
				logger.ErrorContext(ctx, "could not read next node body", "err", err)
				return
			}
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			raftotel.ErrSpan(span, err)
			logger.ErrorContext(ctx, "could not read request body", "err", err)
			w.WriteHeader(500)
			return
		}
		replicateStart := time.Now()
		node.ClientCommand(ctx, body)
		metrics.observeReplicate(replicateStart)
	})
	return handler
}
