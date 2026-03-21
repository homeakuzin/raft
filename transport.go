package raft

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
)

type RPCClient interface {
	IssueRequestVote(ctx context.Context, data RequestVote, node NodeId) (RequestVoteResult, error)
	IssueAppendEntries(ctx context.Context, data AppendEntries, node NodeId) (AppendEntriesResult, error)
}

type RPCServer interface {
	Serve(node *Node) error
	Shutdown(ctx context.Context) error
}

type Transport interface {
	RPCClient
	RPCServer
}

type httpTransport struct {
	id        NodeId
	nodeAddrs map[NodeId]string
	server    *http.Server
	logger    *log.Logger
}

func HTTPTransport(id NodeId, nodeAddrs map[NodeId]string) Transport {
	return &httpTransport{id: id, nodeAddrs: nodeAddrs, logger: log.Default()}
}

func (t *httpTransport) Shutdown(ctx context.Context) error {
	return t.server.Shutdown(ctx)
}

func (t *httpTransport) handlerRequestVote(w http.ResponseWriter, r *http.Request, node *Node) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		t.logger.Printf("could not read body: %s", err.Error())
		w.WriteHeader(500)
		return
	}
	var requestVote RequestVote
	if err := json.Unmarshal(body, &requestVote); err != nil {
		t.logger.Printf("invalid body: %s", err.Error())
		w.WriteHeader(400)
		return
	}
	responseCh := make(chan RequestVoteResult)
	node.requestVoteRpc <- requestVoteRpcCall{requestVote, responseCh}
	response := <-responseCh
	responseBody, err := json.Marshal(&response)
	if err != nil {
		t.logger.Printf("could not serialize response body: %s", err.Error())
		w.WriteHeader(500)
		return
	}
	if _, err := w.Write(responseBody); err != nil {
		t.logger.Printf("could not write response body: %s", err.Error())
		w.WriteHeader(500)
		return
	}
}

func (t *httpTransport) handlerAppendEntries(w http.ResponseWriter, r *http.Request, node *Node) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		t.logger.Printf("could not read body: %s", err.Error())
		w.WriteHeader(500)
		return
	}
	var appendEntries AppendEntries
	if err := json.Unmarshal(body, &appendEntries); err != nil {
		t.logger.Printf("invalid body: %s", err.Error())
		w.WriteHeader(400)
		return
	}
	responseCh := make(chan AppendEntriesResult)
	node.appendEntriesRpc <- appendEntriesRpcCall{appendEntries, responseCh}
	response := <-responseCh
	responseBody, err := json.Marshal(&response)
	if err != nil {
		t.logger.Printf("could not serialize response body: %s", err.Error())
		w.WriteHeader(500)
		return
	}
	if _, err := w.Write(responseBody); err != nil {
		t.logger.Printf("could not write response body: %s", err.Error())
		w.WriteHeader(500)
		return
	}
}

func (t *httpTransport) Serve(node *Node) error {
	handler := http.NewServeMux()
	handler.HandleFunc("POST /rpc/request-vote", func(w http.ResponseWriter, r *http.Request) {
		t.handlerRequestVote(w, r, node)
	})
	handler.HandleFunc("POST /rpc/append-entries", func(w http.ResponseWriter, r *http.Request) {
		t.handlerAppendEntries(w, r, node)
	})
	host := t.nodeAddrs[t.id]
	t.logger.Printf("Running HTTP server at %v", host)
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
