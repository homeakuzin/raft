package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
)

type httpPeer struct {
	addr   string
	client *http.Client
}

type HttpPeerTransport struct {
	ln      net.Listener
	nodeId  NodeId
	peers   map[NodeId]httpPeer
	server  *http.Server
	logger  *RaftLogger
	handler *httpHandler
}

func NewHttpTransport(ln net.Listener, nodeId NodeId, peers map[NodeId]string, logger *RaftLogger) *HttpPeerTransport {
	handler := &httpHandler{}
	httpPeers := make(map[NodeId]httpPeer, len(peers))
	for id, addr := range peers {
		httpPeers[id] = httpPeer{addr: addr, client: &http.Client{}}
	}
	return &HttpPeerTransport{
		ln:      ln,
		nodeId:  nodeId,
		peers:   httpPeers,
		logger:  logger,
		server:  &http.Server{Addr: ln.Addr().String(), Handler: handler},
		handler: handler,
	}
}

func (t *HttpPeerTransport) Serve(ctx context.Context, requestVoteCallback func(args RequestVoteArgs, replyCh chan<- RequestVoteReply), appendEntriesCallback func(args AppendEntriesArgs, replyCh chan<- AppendEntriesReply)) error {
	t.handler.requestVoteCallback = requestVoteCallback
	t.handler.appendEntriesCallback = appendEntriesCallback
	t.handler.logger = t.logger
	t.logger.InfoContext(ctx, "starting http transport", "addr", t.ln.Addr())
	go t.server.Serve(t.ln)
	return nil
}

func (t *HttpPeerTransport) Shutdown(ctx context.Context) {
	t.server.Close()
}

type httpHandler struct {
	requestVoteCallback   func(args RequestVoteArgs, replyCh chan<- RequestVoteReply)
	appendEntriesCallback func(args AppendEntriesArgs, replyCh chan<- AppendEntriesReply)
	logger                *RaftLogger
}

func (h httpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.WriteHeader(404)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(500)
		h.logger.ErrorContext(r.Context(), "could not read request body", "error", err, "uri", r.RequestURI, "remote_addr", r.RemoteAddr)
		return
	}

	if r.RequestURI == "/request-vote" {
		var args RequestVoteArgs
		if err := json.Unmarshal(body, &args); err != nil {
			w.WriteHeader(400)
		}
		replyCh := make(chan RequestVoteReply, 1)
		h.requestVoteCallback(args, replyCh)
		reply := <-replyCh
		responseBody, err := json.Marshal(&reply)
		if err != nil {
			w.WriteHeader(500)
			h.logger.ErrorContext(r.Context(), "could not encode request body", "error", err)
		}
		w.WriteHeader(200)
		w.Write(responseBody)
	} else if r.RequestURI == "/append-entries" {
		var args AppendEntriesArgs
		if err := json.Unmarshal(body, &args); err != nil {
			w.WriteHeader(400)
		}
		replyCh := make(chan AppendEntriesReply, 1)
		h.appendEntriesCallback(args, replyCh)
		reply := <-replyCh
		responseBody, err := json.Marshal(&reply)
		if err != nil {
			w.WriteHeader(500)
			h.logger.ErrorContext(r.Context(), "could not encode request body", "error", err)
		}
		w.WriteHeader(200)
		w.Write(responseBody)
	}
}

func (t *HttpPeerTransport) RequestVote(ctx context.Context, peer NodeId, data RequestVoteArgs) (RequestVoteReply, error) {
	payload, err := json.Marshal(data)
	if err != nil {
		return RequestVoteReply{}, fmt.Errorf("marshal: %w", err)
	}
	req, _ := http.NewRequestWithContext(ctx, "POST", "http://"+t.peers[peer].addr+"/request-vote", bytes.NewBuffer(payload))
	resp, err := t.peers[peer].client.Do(req)
	if err != nil {
		return RequestVoteReply{}, fmt.Errorf("POST /request-vote: %w", err)
	}
	defer resp.Body.Close()
	resultBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return RequestVoteReply{}, fmt.Errorf("read POST /request-vote response: %w", err)
	}
	if resp.StatusCode != 200 {
		return RequestVoteReply{}, fmt.Errorf("POST /request-vote status code %d: %s", resp.StatusCode, resultBytes[:min(len(resultBytes), 64)])
	}
	var result RequestVoteReply
	if err := json.Unmarshal(resultBytes, &result); err != nil {
		return RequestVoteReply{}, fmt.Errorf("POST /request-vote returned invalid JSON: %s (%w)", resultBytes[:min(len(resultBytes), 64)], err)
	}
	return result, nil
}

func (t *HttpPeerTransport) AppendEntries(ctx context.Context, peer NodeId, data AppendEntriesArgs) (AppendEntriesReply, error) {
	payload, err := json.Marshal(data)
	if err != nil {
		return AppendEntriesReply{}, fmt.Errorf("marshal: %w", err)
	}
	req, _ := http.NewRequestWithContext(ctx, "POST", "http://"+t.peers[peer].addr+"/append-entries", bytes.NewBuffer(payload))
	resp, err := t.peers[peer].client.Do(req)
	if err != nil {
		return AppendEntriesReply{}, fmt.Errorf("POST /append-entries: %w", err)
	}
	defer resp.Body.Close()
	resultBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return AppendEntriesReply{}, fmt.Errorf("read POST /append-entries response: %w", err)
	}
	if resp.StatusCode != 200 {
		return AppendEntriesReply{}, fmt.Errorf("POST /append-entries status code %d: %s", resp.StatusCode, resultBytes[:min(len(resultBytes), 64)])
	}
	var result AppendEntriesReply
	if err := json.Unmarshal(resultBytes, &result); err != nil {
		return AppendEntriesReply{}, fmt.Errorf("POST /append-entries returned invalid JSON: %s (%w)", resultBytes[:min(len(resultBytes), 64)], err)
	}
	return result, nil
}
