package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
)

// Http2PeerTransport exchanges Raft RPCs over HTTP/2 with TLS.
// certFile is a PEM certificate trusted by all peers (or a chain rooted in their trust pool).
// Peer addresses must contain host:port with a host covered by the certificate's SAN.
type Http2PeerTransport struct {
	ln     net.Listener
	peers  map[NodeId]httpPeer
	server *http.Server
	logger *RaftLogger
}

var _ Transport = (*Http2PeerTransport)(nil)

func NewHttp2Transport(ln net.Listener, nodeId NodeId, peers map[NodeId]string, logger *RaftLogger, certFile, keyFile string) (*Http2PeerTransport, error) {
	certPEM, err := os.ReadFile(certFile)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", certFile, err)
	}
	keyPEM, err := os.ReadFile(keyFile)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", keyFile, err)
	}
	certificate, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("load TLS key pair: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certPEM) {
		return nil, fmt.Errorf("parse %s: no valid PEM certificates found", certFile)
	}
	protocols := new(http.Protocols)
	protocols.SetHTTP2(true)
	httpPeers := make(map[NodeId]httpPeer, len(peers))
	for id, addr := range peers {
		httpPeers[id] = httpPeer{addr: addr, client: &http.Client{
			Transport: &http.Transport{
				Protocols:       protocols,
				TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12},
			},
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse },
		}}
	}
	return &Http2PeerTransport{
		ln: ln, peers: httpPeers, logger: logger,
		server: &http.Server{
			Addr: ln.Addr().String(), Protocols: protocols,
			TLSConfig: &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12},
		},
	}, nil
}

func (t *Http2PeerTransport) Serve(ctx context.Context, requestVoteCallback func(RequestVoteArgs, chan<- RequestVoteReply), appendEntriesCallback func(AppendEntriesArgs, chan<- AppendEntriesReply)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /request-vote", func(w http.ResponseWriter, r *http.Request) {
		var args RequestVoteArgs
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "could not read request", http.StatusBadRequest)
			return
		}
		if err := json.Unmarshal(body, &args); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		replyCh := make(chan RequestVoteReply, 1)
		requestVoteCallback(args, replyCh)
		select {
		case reply := <-replyCh:
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(reply); err != nil {
				t.logger.ErrorContext(r.Context(), "write request-vote response", "error", err)
			}
		case <-r.Context().Done():
		}
	})
	mux.HandleFunc("POST /append-entries", func(w http.ResponseWriter, r *http.Request) {
		var args AppendEntriesArgs
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "could not read request", http.StatusBadRequest)
			return
		}
		if err := json.Unmarshal(body, &args); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		replyCh := make(chan AppendEntriesReply, 1)
		appendEntriesCallback(args, replyCh)
		select {
		case reply := <-replyCh:
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(reply); err != nil {
				t.logger.ErrorContext(r.Context(), "write append-entries response", "error", err)
			}
		case <-r.Context().Done():
		}
	})
	t.server.Handler = mux
	t.server.BaseContext = func(net.Listener) context.Context { return ctx }
	t.logger.InfoContext(ctx, "starting http2 transport", "addr", t.ln.Addr())
	go func() {
		if err := t.server.ServeTLS(t.ln, "", ""); err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.logger.ErrorContext(ctx, "http2 transport stopped", "error", err)
		}
	}()
	return nil
}

func (t *Http2PeerTransport) Shutdown(ctx context.Context) {
	if err := t.server.Close(); err != nil {
		t.logger.ErrorContext(ctx, "close http2 server", "error", err)
	}
	for _, peer := range t.peers {
		peer.client.CloseIdleConnections()
	}
}

func (t *Http2PeerTransport) RequestVote(ctx context.Context, peer NodeId, data RequestVoteArgs) (RequestVoteReply, error) {
	p, ok := t.peers[peer]
	if !ok {
		return RequestVoteReply{}, fmt.Errorf("unknown peer %v", peer)
	}
	payload, err := json.Marshal(data)
	if err != nil {
		return RequestVoteReply{}, fmt.Errorf("marshal: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, "POST", "https://"+p.addr+"/request-vote", bytes.NewBuffer(payload))
	if err != nil {
		return RequestVoteReply{}, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.client.Do(req)
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

func (t *Http2PeerTransport) AppendEntries(ctx context.Context, peer NodeId, data AppendEntriesArgs) (AppendEntriesReply, error) {
	p, ok := t.peers[peer]
	if !ok {
		return AppendEntriesReply{}, fmt.Errorf("unknown peer %v", peer)
	}
	payload, err := json.Marshal(data)
	if err != nil {
		return AppendEntriesReply{}, fmt.Errorf("marshal: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, "POST", "https://"+p.addr+"/append-entries", bytes.NewBuffer(payload))
	if err != nil {
		return AppendEntriesReply{}, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.client.Do(req)
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
