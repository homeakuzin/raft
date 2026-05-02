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
)

type RPCClient interface {
	IssueRequestVote(ctx context.Context, data RequestVote, node NodeId) (RequestVoteResult, error)
	IssueAppendEntries(ctx context.Context, data AppendEntries, node NodeId) (AppendEntriesResult, error)
}

type RaftProtocol interface {
	RequestVote(RequestVote) (RequestVoteResult, error)
	AppendEntries(AppendEntries) (AppendEntriesResult, error)
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
	id        NodeId
	nodeAddrs map[NodeId]string
	server    *http.Server
	logger    *slog.Logger
}

func HTTPTransport(id NodeId, nodeAddrs map[NodeId]string, logger *slog.Logger) Transport {
	return &httpTransport{id: id, nodeAddrs: nodeAddrs, logger: logger.With("node", id.String())}
}

func (t *httpTransport) ShutdownServer(ctx context.Context) error {
	return t.server.Close()
	// return t.server.Shutdown(ctx)
}

func (t *httpTransport) handlerRequestVote(w http.ResponseWriter, r *http.Request, protocol RaftProtocol) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		t.logger.Error("could not read body", "error", err.Error())
		w.WriteHeader(500)
		return
	}
	var requestVote RequestVote
	if err := json.Unmarshal(body, &requestVote); err != nil {
		t.logger.Info("invalid body", "error", err.Error())
		w.WriteHeader(400)
		return
	}
	response, err := protocol.RequestVote(requestVote)
	if err != nil {
		t.logger.Error("could not handle RequestVote RPC", "error", err.Error())
		w.WriteHeader(500)
		return
	}
	responseBody, err := json.Marshal(&response)
	if err != nil {
		t.logger.Info("could not serialize response body", "error", err.Error())
		w.WriteHeader(500)
		return
	}
	if _, err := w.Write(responseBody); err != nil {
		t.logger.Error("could not write response body", "error", err.Error())
		w.WriteHeader(500)
		return
	}
}

func (t *httpTransport) handlerAppendEntries(w http.ResponseWriter, r *http.Request, protocol RaftProtocol) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		t.logger.Error("could not read body", "error", err.Error())
		w.WriteHeader(500)
		return
	}
	var appendEntries AppendEntries
	if err := json.Unmarshal(body, &appendEntries); err != nil {
		t.logger.Info("invalid body", "error", err.Error())
		w.WriteHeader(400)
		return
	}
	response, err := protocol.AppendEntries(appendEntries)
	if err != nil {
		t.logger.Error("could not handle AppendEntries RPC", "error", err.Error())
		w.WriteHeader(500)
		return
	}
	responseBody, err := json.Marshal(&response)
	if err != nil {
		t.logger.Info("could not serialize response body", "error", err.Error())
		w.WriteHeader(500)
		return
	}
	if _, err := w.Write(responseBody); err != nil {
		t.logger.Error("could not write response body", "error", err.Error())
		w.WriteHeader(500)
		return
	}
}

func (t *httpTransport) Serve(protocol RaftProtocol) error {
	handler := http.NewServeMux()
	handler.HandleFunc("POST /rpc/request-vote", func(w http.ResponseWriter, r *http.Request) {
		t.handlerRequestVote(w, r, protocol)
	})
	handler.HandleFunc("POST /rpc/append-entries", func(w http.ResponseWriter, r *http.Request) {
		t.handlerAppendEntries(w, r, protocol)
	})
	host := t.nodeAddrs[t.id]
	t.logger.Info("Running HTTP server", "host", host)
	t.server = &http.Server{Addr: host, Handler: handler}

	addr := host
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	go t.server.Serve(ln)
	return nil
}

func (t *httpTransport) IssueRequestVote(ctx context.Context, data RequestVote, node NodeId) (result RequestVoteResult, err error) {
	body, err := json.Marshal(&data)
	if err != nil {
		return
	}

	host := t.nodeAddrs[node]
	req, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("http://%s/rpc/request-vote", host), bytes.NewBuffer(body))
	if err != nil {
		return
	}
	client := http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		err = fmt.Errorf("node returned %d status code", resp.StatusCode)
		return
	}
	resultBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return
	}
	if err = json.Unmarshal(resultBytes, &result); err != nil {
		return
	}
	return
}

func (t *httpTransport) IssueAppendEntries(ctx context.Context, data AppendEntries, node NodeId) (AppendEntriesResult, error) {
	var result AppendEntriesResult
	body, err := json.Marshal(&data)
	if err != nil {
		return result, err
	}

	host := t.nodeAddrs[node]
	req, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("http://%s/rpc/append-entries", host), bytes.NewBuffer(body))
	if err != nil {
		return result, err
	}
	client := http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return result, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		err = fmt.Errorf("node returned %d status code", resp.StatusCode)
		return result, err
	}
	resultBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return result, err
	}
	err = json.Unmarshal(resultBytes, &result)
	return result, err
}
