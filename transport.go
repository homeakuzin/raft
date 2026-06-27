package raft

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	raftotel "github.com/homeakuzin/raft/pkg/otel"
)

type RPCClient interface {
	IssueRequestVote(ctx context.Context, data RequestVote, node NodeId) (RequestVoteResult, error)
	IssueAppendEntries(ctx context.Context, data AppendEntries, node NodeId) (AppendEntriesResult, error)
}

type RaftProtocol interface {
	RequestVote(context.Context, RequestVote) (RequestVoteResult, error)
	AppendEntries(context.Context, AppendEntries) (AppendEntriesResult, error)
}

type RPCServer interface {
	Serve(protocol RaftProtocol) error
	ShutdownServer(ctx context.Context) error
}

type Transport interface {
	RPCClient
	RPCServer
}

type httpTransport struct {
	id         NodeId
	listenAddr string
	authToken  string
	nodeAddrs  map[NodeId]string
	server     *http.Server
	logger     *slog.Logger
}

func HTTPTransport(id NodeId, listenAddr string, nodeAddrs map[NodeId]string, logger *slog.Logger, authToken string) Transport {
	return &httpTransport{id: id, listenAddr: listenAddr, authToken: authToken, nodeAddrs: nodeAddrs, logger: logger.With("node", id.String())}
}

func (t *httpTransport) ShutdownServer(ctx context.Context) error {
	return t.server.Close()
	// return t.server.Shutdown(ctx)
}

func (t *httpTransport) handlerRequestVote(w http.ResponseWriter, r *http.Request, protocol RaftProtocol) {
	ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
	ctx, span := otel.Tracer("raft").Start(ctx, "RequestVote Handler", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	body, err := io.ReadAll(r.Body)
	if err != nil {
		t.logger.ErrorContext(ctx, "could not read body", "error", err.Error())
		w.WriteHeader(500)
		return
	}
	var requestVote RequestVote
	if err := json.Unmarshal(body, &requestVote); err != nil {
		t.logger.InfoContext(ctx, "invalid body", "error", err.Error())
		w.WriteHeader(400)
		return
	}
	response, err := protocol.RequestVote(ctx, requestVote)
	if err != nil {
		t.logger.ErrorContext(ctx, "could not handle RequestVote RPC", "error", err.Error())
		w.WriteHeader(500)
		return
	}
	responseBody, err := json.Marshal(&response)
	if err != nil {
		t.logger.InfoContext(ctx, "could not serialize response body", "error", err.Error())
		w.WriteHeader(500)
		return
	}
	if _, err := w.Write(responseBody); err != nil {
		t.logger.ErrorContext(ctx, "could not write response body", "error", err.Error())
		w.WriteHeader(500)
		return
	}
}

func (t *httpTransport) handlerAppendEntries(w http.ResponseWriter, r *http.Request, protocol RaftProtocol) {
	ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
	ctx, span := otel.Tracer("raft").Start(ctx, "AppendEntries handler", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	body, err := io.ReadAll(r.Body)
	if err != nil {
		t.logger.ErrorContext(ctx, "could not read body", "error", err.Error())
		w.WriteHeader(500)
		return
	}
	var appendEntries AppendEntries
	if err := json.Unmarshal(body, &appendEntries); err != nil {
		t.logger.InfoContext(ctx, "invalid body", "error", err.Error())
		w.WriteHeader(400)
		return
	}
	response, err := protocol.AppendEntries(ctx, appendEntries)
	if err != nil {
		t.logger.ErrorContext(ctx, "could not handle AppendEntries RPC", "error", err.Error())
		w.WriteHeader(500)
		return
	}
	responseBody, err := json.Marshal(&response)
	if err != nil {
		t.logger.InfoContext(ctx, "could not serialize response body", "error", err.Error())
		w.WriteHeader(500)
		return
	}
	if _, err := w.Write(responseBody); err != nil {
		t.logger.ErrorContext(ctx, "could not write response body", "error", err.Error())
		w.WriteHeader(500)
		return
	}
}

func (t *httpTransport) Serve(protocol RaftProtocol) error {
	handler := http.NewServeMux()
	handler.HandleFunc("POST /rpc/request-vote", func(w http.ResponseWriter, r *http.Request) {
		if t.authenticated(w, r) {
			t.handlerRequestVote(w, r, protocol)
		}
	})
	handler.HandleFunc("POST /rpc/append-entries", func(w http.ResponseWriter, r *http.Request) {
		if t.authenticated(w, r) {
			t.handlerAppendEntries(w, r, protocol)
		}
	})
	t.logger.Info("Running node HTTP server", "host", t.listenAddr)
	t.server = &http.Server{Addr: t.listenAddr, Handler: handler}

	ln, err := net.Listen("tcp", t.listenAddr)
	if err != nil {
		return err
	}
	go t.server.Serve(ln)
	return nil
}

func (t *httpTransport) IssueRequestVote(ctx context.Context, data RequestVote, node NodeId) (result RequestVoteResult, err error) {
	ctx, span := otel.Tracer("raft").Start(ctx, "RequestVote", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	body, err := json.Marshal(&data)
	if err != nil {
		raftotel.ErrSpan(span, err)
		return result, err
	}

	host := t.nodeAddrs[node]
	req, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("http://%s/rpc/request-vote", host), bytes.NewBuffer(body))
	if err != nil {
		raftotel.ErrSpan(span, err)
		return result, err
	}
	client := http.Client{}
	req.Header.Set(httpAuthTokenHeader, t.authToken)
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
	resp, err := client.Do(req)
	if err != nil {
		raftotel.ErrSpan(span, err)
		return result, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		err = fmt.Errorf("node returned %d status code", resp.StatusCode)
		raftotel.ErrSpan(span, err)
		return result, err
	}
	resultBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		raftotel.ErrSpan(span, err)
		return result, err
	}
	if err = json.Unmarshal(resultBytes, &result); err != nil {
		raftotel.ErrSpan(span, err)
		return result, err
	}
	return
}

func (t *httpTransport) IssueAppendEntries(ctx context.Context, data AppendEntries, node NodeId) (AppendEntriesResult, error) {
	var result AppendEntriesResult
	ctx, span := otel.Tracer("raft").Start(ctx, "AppendEntries", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	body, err := json.Marshal(&data)
	if err != nil {
		raftotel.ErrSpan(span, err)
		return result, err
	}

	host := t.nodeAddrs[node]
	req, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("http://%s/rpc/append-entries", host), bytes.NewBuffer(body))
	if err != nil {
		raftotel.ErrSpan(span, err)
		return result, err
	}
	client := http.Client{}
	req.Header.Set(httpAuthTokenHeader, t.authToken)
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
	resp, err := client.Do(req)
	if err != nil {
		raftotel.ErrSpan(span, err)
		return result, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		err = fmt.Errorf("node returned %d status code", resp.StatusCode)
		raftotel.ErrSpan(span, err)
		return result, err
	}
	resultBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		raftotel.ErrSpan(span, err)
		return result, err
	}
	err = json.Unmarshal(resultBytes, &result)
	if err != nil {
		raftotel.ErrSpan(span, err)
	}
	return result, err
}

const httpAuthTokenHeader = "X-Raft-Auth-Token"

func (t *httpTransport) authenticated(w http.ResponseWriter, r *http.Request) bool {
	if r.Header.Get(httpAuthTokenHeader) == "" {
		w.WriteHeader(401)
		return false
	}
	return true
}
