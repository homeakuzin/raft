package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type NodeId int

const (
	None NodeId = iota
	Node1
	Node2
	Node3
)

type State string

const (
	Follower  State = "follower"
	Candidate State = "candidate"
	Leader    State = "leader"
)

type NodeTimeouts struct {
	Election  time.Duration
	Heartbeat time.Duration
}

var DefaultNodeTimeouts = NodeTimeouts{
	Election:  150 * time.Millisecond,
	Heartbeat: 50 * time.Millisecond,
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  NodeId
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Peer        NodeId
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId NodeId
}

type AppendEntriesReply struct {
	Peer    NodeId
	Term    int
	Success bool
}

type requestVoteRpc struct {
	args    RequestVoteArgs
	replyCh chan<- RequestVoteReply
}

type appendEntriesRpc struct {
	args    AppendEntriesArgs
	replyCh chan<- AppendEntriesReply
}

type Transport interface {
	Serve(ctx context.Context, requestVoteCallback func(args RequestVoteArgs, replyCh chan<- RequestVoteReply), appendEntriesCallback func(args AppendEntriesArgs, replyCh chan<- AppendEntriesReply)) error
	Shutdown(ctx context.Context)
	RequestVote(ctx context.Context, id NodeId, data RequestVoteArgs) (RequestVoteReply, error)
	AppendEntries(ctx context.Context, id NodeId, data AppendEntriesArgs) (AppendEntriesReply, error)
}

type Node struct {
	mu          *sync.Mutex
	id          NodeId
	votedFor    NodeId
	currentTerm int
	state       State
	transport   Transport
	logger      *RaftLogger
	peers       []NodeId
	shutdownCh  chan struct{}
	runWg       sync.WaitGroup
	rpcWg       sync.WaitGroup

	timeouts NodeTimeouts

	currentElectionVotes map[NodeId]bool
	requestVoteRpcCh     chan requestVoteRpc
	appendEntriesRpcCh   chan appendEntriesRpc
	requestVoteReplyCh   chan RequestVoteReply
	appendEntriesReplyCh chan AppendEntriesReply
}

func NewNode(id NodeId, peers []NodeId, logger *RaftLogger, transport Transport) *Node {
	return &Node{
		mu:         &sync.Mutex{},
		id:         id,
		logger:     logger,
		peers:      peers,
		transport:  transport,
		shutdownCh: make(chan struct{}),
		timeouts:   DefaultNodeTimeouts,
	}
}

func (n *Node) Id() NodeId {
	return n.id
}

func (n *Node) SetTimeouts(timeouts NodeTimeouts) *Node {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.timeouts = timeouts
	return n
}

func (n *Node) State() State {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state
}

func (n *Node) CurrentTerm() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm
}

func (n *Node) Shutdown(ctx context.Context) {
	close(n.shutdownCh)
	n.transport.Shutdown(ctx)
	n.rpcWg.Wait()
	n.runWg.Wait()
}

func (n *Node) Run(ctx context.Context) error {
	n.runWg.Add(1)
	defer n.runWg.Done()

	n.state = Follower
	n.logger.InfoContext(ctx, "start node")
	n.requestVoteRpcCh = make(chan requestVoteRpc, len(n.peers))
	n.requestVoteReplyCh = make(chan RequestVoteReply, len(n.peers))
	n.appendEntriesRpcCh = make(chan appendEntriesRpc, len(n.peers))
	n.appendEntriesReplyCh = make(chan AppendEntriesReply, len(n.peers))
	n.currentElectionVotes = make(map[NodeId]bool, len(n.peers))
	n.votedFor = -1
	if err := n.transport.Serve(ctx, func(args RequestVoteArgs, replyCh chan<- RequestVoteReply) {
		n.logger.dlog2("received RequestVote", "args", args)
		n.requestVoteRpcCh <- requestVoteRpc{args, replyCh}
	}, func(args AppendEntriesArgs, replyCh chan<- AppendEntriesReply) {
		n.logger.dlog2("received AppendEntries", "args", args)
		n.appendEntriesRpcCh <- appendEntriesRpc{args, replyCh}
	}); err != nil {
		n.logger.ErrorContext(ctx, "could not serve raft transport", "error", err)
		return fmt.Errorf("could not serve raft transport: %w", err)
	}
	defer n.transport.Shutdown(ctx)
	for {
		if n.eventLoop(ctx) {
			return nil
		}
	}
}

func (n *Node) eventLoop(ctx context.Context) (stop bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	electionTimeout := n.timeouts.Election
	electionTimeout += time.Duration(rand.Int63n(int64(n.timeouts.Election)) * 2)
	election := time.NewTimer(electionTimeout)
	defer election.Stop()

	heartbeat := time.NewTimer(n.timeouts.Heartbeat)
	defer heartbeat.Stop()
	if n.state != Leader {
		heartbeat.Stop()
	}

	select {
	case <-ctx.Done():
		stop = true
	case <-n.shutdownCh:
		stop = true

	case <-heartbeat.C:
		n.logger.dlog("heartbeat timeout")
		n.sendHeartbeats(ctx, n.appendEntriesReplyCh)
	case reply := <-n.appendEntriesReplyCh:
		n.logger.dlog2("handle AppendEntries reply", "peer", reply.Peer, "reply", reply)
		if reply.Term > n.currentTerm {
			n.becomeFollower(reply.Term)
		}
	case appendEntries := <-n.appendEntriesRpcCh:
		n.logger.dlog2("handle AppendEntries call", "peer", appendEntries.args.LeaderId, "args", appendEntries.args)
		var reply AppendEntriesReply
		reply.Peer = n.id
		reply.Term = n.currentTerm
		reply.Success = false
		if appendEntries.args.Term >= n.currentTerm {
			reply.Success = true
			n.becomeFollower(appendEntries.args.Term)
		}
		appendEntries.replyCh <- reply

	case <-election.C:
		n.logger.dlog("start election", "new_term", n.currentTerm+1, "timeout", electionTimeout)
		n.startElection(ctx, n.requestVoteReplyCh)
	case reply := <-n.requestVoteReplyCh:
		n.logger.dlog2("handle RequestVote reply", "peer", reply.Peer, "reply", reply)
		if reply.Term > n.currentTerm {
			n.becomeFollower(reply.Term)
			return
		}
		if reply.VoteGranted && n.state == Candidate {
			n.currentElectionVotes[reply.Peer] = true
			votes := 1
			for _, voteGranted := range n.currentElectionVotes {
				if voteGranted {
					votes++
				}
			}
			if votes*2 > len(n.peers)+1 {
				n.logger.dlog("become leader")
				n.state = Leader
				n.sendHeartbeats(ctx, n.appendEntriesReplyCh)
			}
		}
	case requestVote := <-n.requestVoteRpcCh:
		n.logger.dlog2("handle RequestVote call", "peer", requestVote.args.CandidateId, "args", requestVote.args)
		var reply RequestVoteReply
		reply.VoteGranted = false
		if requestVote.args.Term > n.currentTerm || requestVote.args.Term == n.currentTerm && (n.votedFor == -1 || n.votedFor == requestVote.args.CandidateId) {
			n.becomeFollower(requestVote.args.Term)
			n.votedFor = requestVote.args.CandidateId
			reply.VoteGranted = true
		}
		reply.Peer = n.id
		reply.Term = n.currentTerm
		requestVote.replyCh <- reply
	}
	return
}

func (n *Node) becomeFollower(term int) {
	n.logger.dlog("become follower", "term", term)
	n.state = Follower
	n.currentTerm = term
}

func (n *Node) sendHeartbeats(ctx context.Context, replyCh chan<- AppendEntriesReply) {
	n.logger.dlog("send heartbeats")
	args := AppendEntriesArgs{
		Term:     n.currentTerm,
		LeaderId: n.id,
	}
	for _, peer := range n.peers {
		n.rpcWg.Add(1)
		go func() {
			defer n.rpcWg.Done()
			n.logger.dlog3("send AppendEntries", "peer", peer, "args", args)
			reply, err := n.transport.AppendEntries(ctx, peer, args)
			if err != nil {
				n.logger.dlog("AppendEntries failed", "peer", peer, "error", err)
				return
			}
			reply.Peer = peer
			replyCh <- reply
		}()
	}
}

func (n *Node) startElection(ctx context.Context, replyCh chan<- RequestVoteReply) {
	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.id
	args := RequestVoteArgs{
		Term:         n.currentTerm,
		CandidateId:  n.id,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	for _, peer := range n.peers {
		n.rpcWg.Add(1)
		go func() {
			defer n.rpcWg.Done()
			n.logger.dlog3("send RequestVote", "peer", peer, "args", args)
			reply, err := n.transport.RequestVote(ctx, peer, args)
			if err != nil {
				n.logger.dlog("RequestVote failed", "peer", peer, "error", err)
				return
			}
			reply.Peer = peer
			replyCh <- reply
		}()
	}
}

type SlogLogger interface {
	Debug(msg string, args ...any)
	InfoContext(ctx context.Context, msg string, args ...any)
	ErrorContext(ctx context.Context, msg string, args ...any)
}

type RaftLogger struct {
	SlogLogger
	debugLevel atomic.Int32
}

func NewRaftLogger(logger SlogLogger) *RaftLogger {
	return &RaftLogger{SlogLogger: logger}
}

func (l *RaftLogger) DebugLevel(level int) *RaftLogger {
	l.debugLevel.Store(int32(level))
	return l
}

func (l *RaftLogger) dlog(msg string, args ...any) {
	if l.debugLevel.Load() > 0 {
		l.Debug(msg, args...)
	}
}

func (l *RaftLogger) dlog2(msg string, args ...any) {
	if l.debugLevel.Load() > 1 {
		l.Debug(msg, args...)
	}
}

func (l *RaftLogger) dlog3(msg string, args ...any) {
	if l.debugLevel.Load() > 2 {
		l.Debug(msg, args...)
	}
}

type MemoryTransport struct {
	mu                    sync.RWMutex
	nodeId                NodeId
	peers                 map[NodeId]*MemoryTransport
	requestVoteCallback   func(args RequestVoteArgs, replyCh chan<- RequestVoteReply)
	appendEntriesCallback func(args AppendEntriesArgs, replyCh chan<- AppendEntriesReply)
	shutdownCh            chan struct{}
	shutdown              bool
}

func NewMemoryTransport(nodeId NodeId, peers map[NodeId]*MemoryTransport) *MemoryTransport {
	return &MemoryTransport{
		nodeId:     nodeId,
		peers:      peers,
		shutdownCh: make(chan struct{}),
	}
}

func (t *MemoryTransport) Serve(ctx context.Context, requestVoteCallback func(args RequestVoteArgs, replyCh chan<- RequestVoteReply), appendEntriesCallback func(args AppendEntriesArgs, replyCh chan<- AppendEntriesReply)) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.requestVoteCallback = requestVoteCallback
	t.appendEntriesCallback = appendEntriesCallback
	return nil
}

func (t *MemoryTransport) Shutdown(ctx context.Context) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.shutdown {
		close(t.shutdownCh)
		t.shutdown = true
	}
}

func (t *MemoryTransport) RequestVote(ctx context.Context, peer NodeId, data RequestVoteArgs) (RequestVoteReply, error) {
	peerTransport := t.peers[peer]

	peerTransport.mu.RLock()
	requestVoteCallback := peerTransport.requestVoteCallback
	peerTransport.mu.RUnlock()

	replyCh := make(chan RequestVoteReply, 1)
	requestVoteCallback(data, replyCh)
	select {
	case <-ctx.Done():
		return RequestVoteReply{}, ctx.Err()
	case <-t.shutdownCh:
		return RequestVoteReply{}, fmt.Errorf("memory transport %d is shut down", t.nodeId)
	case <-peerTransport.shutdownCh:
		return RequestVoteReply{}, fmt.Errorf("memory transport peer %d is shut down", peer)
	case reply := <-replyCh:
		return reply, nil
	}
}

func (t *MemoryTransport) AppendEntries(ctx context.Context, peer NodeId, data AppendEntriesArgs) (AppendEntriesReply, error) {
	peerTransport := t.peers[peer]

	peerTransport.mu.RLock()
	appendEntriesCallback := peerTransport.appendEntriesCallback
	peerTransport.mu.RUnlock()

	replyCh := make(chan AppendEntriesReply, 1)
	appendEntriesCallback(data, replyCh)
	select {
	case <-ctx.Done():
		return AppendEntriesReply{}, ctx.Err()
	case <-t.shutdownCh:
		return AppendEntriesReply{}, fmt.Errorf("memory transport %d is shut down", t.nodeId)
	case <-peerTransport.shutdownCh:
		return AppendEntriesReply{}, fmt.Errorf("memory transport peer %d is shut down", peer)
	case reply := <-replyCh:
		return reply, nil
	}
}

type HttpPeerTransport struct {
	ln     net.Listener
	nodeId NodeId
	peers  map[NodeId]string
	server *http.Server
	logger *RaftLogger
}

func NewHttpTransport(ln net.Listener, nodeId NodeId, peers map[NodeId]string, logger *RaftLogger) *HttpPeerTransport {
	return &HttpPeerTransport{ln: ln, nodeId: nodeId, peers: peers, logger: logger}
}

func (t *HttpPeerTransport) Serve(ctx context.Context, requestVoteCallback func(args RequestVoteArgs, replyCh chan<- RequestVoteReply), appendEntriesCallback func(args AppendEntriesArgs, replyCh chan<- AppendEntriesReply)) error {
	t.server = &http.Server{Addr: t.ln.Addr().String(), Handler: httpHandler{requestVoteCallback, appendEntriesCallback, t.logger}}
	go t.server.Serve(t.ln)
	return nil
}

func (t *HttpPeerTransport) Shutdown(ctx context.Context) {
	if t.server != nil {
		t.server.Close()
	}
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
		h.logger.ErrorContext(r.Context(), "could not read request body", "error", err)
	}

	if r.RequestURI == "/request-vote" {
		var args RequestVoteArgs
		if err := json.Unmarshal(body, &args); err != nil {
			w.WriteHeader(400)
		}
		replyCh := make(chan RequestVoteReply)
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
		replyCh := make(chan AppendEntriesReply)
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
	req, _ := http.NewRequestWithContext(ctx, "POST", "http://"+t.peers[peer]+"/request-vote", bytes.NewBuffer(payload))
	resp, err := http.DefaultClient.Do(req)
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
	req, _ := http.NewRequestWithContext(ctx, "POST", "http://"+t.peers[peer]+"/append-entries", bytes.NewBuffer(payload))
	resp, err := http.DefaultClient.Do(req)
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
