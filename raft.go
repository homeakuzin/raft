package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type NodeId string

const (
	None  NodeId = ""
	Node1 NodeId = "1"
	Node2 NodeId = "2"
	Node3 NodeId = "3"
)

func (id NodeId) String() string {
	return "Node" + string(id)
}

type State string

const (
	Follower  State = "follower"
	Candidate State = "candidate"
	Leader    State = "leader"
)

func (s State) Float64() float64 {
	if s == Follower {
		return 0
	} else if s == Candidate {
		return 1
	} else if s == Leader {
		return 2
	} else {
		return -1
	}
}

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
	Term         int
	LeaderId     NodeId
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []Log
}

type AppendEntriesReply struct {
	Peer          NodeId
	Term          int
	Success       bool
	entriesBounds struct {
		from, to int
	}
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

type Log struct {
	Term  int
	Index int
	Data  []byte
}

type clientCommand struct {
	ctx        context.Context
	data       []byte
	replicated chan error
}

// Index starts with 1
type logStorage struct {
	items []Log
}

func (s *logStorage) len() int {
	return len(s.items)
}

func (s *logStorage) slice(from, to int) []Log {
	if from < 1 {
		panic("logStorage index starts with 1")
	}
	return s.items[from-1 : to-1]
}

func (s *logStorage) at(i int) Log {
	if i < 1 {
		panic("logStorage index starts with 1")
	}
	return s.items[i-1]
}

func (s *logStorage) clearFrom(i int) {
	if i < 1 {
		panic("logStorage index starts with 1")
	}
	s.items = s.items[:i-1]
}

func (s *logStorage) append(log ...Log) {
	s.items = append(s.items, log...)
}

type StateMachine struct {
	mu   sync.Mutex
	logs []Log
}

func (sm *StateMachine) Len() int {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return len(sm.logs)
}

func (sm *StateMachine) Logs() []Log {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	logs := make([]Log, len(sm.logs))
	copy(logs, sm.logs)
	return logs
}

func (sm *StateMachine) apply(logs ...Log) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.logs = append(sm.logs, logs...)
}

type Node struct {
	mu           *sync.Mutex
	id           NodeId
	votedFor     NodeId
	currentTerm  int
	state        State
	logStorage   *logStorage
	stateMachine *StateMachine

	commitIndex           int
	lastApplied           int
	nextIndex             map[NodeId]int
	clientCommandCh       chan clientCommand
	clientCommandIndexMap map[int]clientCommand

	peers          []NodeId
	transport      Transport
	logger         *RaftLogger
	shutdownCh     chan struct{}
	runWg          sync.WaitGroup
	heartbeatTimer *time.Timer
	electionTimer  *time.Timer

	timeouts NodeTimeouts

	currentElectionVotes map[NodeId]bool
	requestVoteRpcCh     chan requestVoteRpc
	appendEntriesRpcCh   chan appendEntriesRpc
	requestVoteReplyCh   chan RequestVoteReply
	appendEntriesReplyCh chan AppendEntriesReply

	electionSpan trace.Span
}

func NewNode(id NodeId, peers []NodeId, logger *RaftLogger, transport Transport) *Node {
	return &Node{
		mu:                    &sync.Mutex{},
		id:                    id,
		logger:                logger,
		peers:                 peers,
		transport:             transport,
		shutdownCh:            make(chan struct{}),
		timeouts:              DefaultNodeTimeouts,
		logStorage:            &logStorage{},
		stateMachine:          &StateMachine{},
		clientCommandIndexMap: make(map[int]clientCommand),
	}
}

func (n *Node) ClientCommand(ctx context.Context, command []byte) error {
	c := clientCommand{ctx, command, make(chan error, 1)}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case n.clientCommandCh <- c:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-c.replicated:
		return err
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

func (n *Node) StateMachine() *StateMachine {
	return n.stateMachine
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

func (n *Node) CommitIndex() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.commitIndex
}

func (n *Node) Shutdown(ctx context.Context) {
	close(n.shutdownCh)
	n.transport.Shutdown(ctx)
	n.runWg.Wait()
}

func (n *Node) Run(ctx context.Context) error {
	n.runWg.Add(1)
	defer n.runWg.Done()

	n.logger.InfoContext(ctx, "start node")

	n.requestVoteRpcCh = make(chan requestVoteRpc, len(n.peers))
	n.requestVoteReplyCh = make(chan RequestVoteReply, len(n.peers))
	n.appendEntriesRpcCh = make(chan appendEntriesRpc, len(n.peers))
	n.appendEntriesReplyCh = make(chan AppendEntriesReply, len(n.peers))
	n.clientCommandCh = make(chan clientCommand)

	n.currentElectionVotes = make(map[NodeId]bool, len(n.peers))
	n.nextIndex = make(map[NodeId]int)

	for _, id := range n.peers {
		n.nextIndex[id] = 1
	}

	if err := n.transport.Serve(ctx, func(args RequestVoteArgs, replyCh chan<- RequestVoteReply) {
		n.requestVoteRpcCh <- requestVoteRpc{args, replyCh}
	}, func(args AppendEntriesArgs, replyCh chan<- AppendEntriesReply) {
		n.appendEntriesRpcCh <- appendEntriesRpc{args, replyCh}
	}); err != nil {
		n.logger.ErrorContext(ctx, "could not serve raft transport", "error", err)
		return fmt.Errorf("could not serve raft transport: %w", err)
	}
	defer n.transport.Shutdown(ctx)

	n.votedFor = None
	n.state = Follower
	n.heartbeatTimer = time.NewTimer(0)
	n.heartbeatTimer.Stop()
	defer n.heartbeatTimer.Stop()
	n.electionTimer = time.NewTimer(0)
	defer n.electionTimer.Stop()
	n.resetElectionTimer()

	for {
		if n.eventLoop(ctx) {
			return nil
		}
	}
}

func (n *Node) eventLoop(ctx context.Context) (stop bool) {
	n.logger.dlog3("wait next event")
	n.mu.Lock()
	defer n.mu.Unlock()

	select {
	case <-ctx.Done():
		stop = true
	case <-n.shutdownCh:
		stop = true

	case <-n.heartbeatTimer.C:
		n.sendAppendEntries(ctx)
		n.resetHeartbeatTimer()
	case reply := <-n.appendEntriesReplyCh:
		n.logger.dlog2("handle AppendEntries reply", "peer", reply.Peer, "reply", reply, "next_index", n.nextIndex)
		if reply.Term > n.currentTerm {
			n.becomeFollower(reply.Term, reply.Peer)
			return
		}
		if reply.Success {
			if reply.entriesBounds.to > 0 && reply.entriesBounds.to >= reply.entriesBounds.from {
				n.nextIndex[reply.Peer] = reply.entriesBounds.to + 1

				matched := make([]int, 0, len(n.nextIndex))
				for _, idx := range n.nextIndex {
					matched = append(matched, idx-1)
				}
				slices.Sort(matched)

				var quorumMatchIndex int
				if len(n.nextIndex) == 2 {
					quorumMatchIndex = matched[1]
				}

				if quorumMatchIndex > n.commitIndex {
					oldIndex := n.commitIndex
					newLogs := n.logStorage.slice(oldIndex+1, quorumMatchIndex+1)
					n.logger.dlog("update commit index", "old_commit_index", oldIndex, "newCommitIndex", quorumMatchIndex, "new_logs_count", len(newLogs))
					n.stateMachine.apply(newLogs...)
					n.commitIndex = quorumMatchIndex
					for i := oldIndex; i <= quorumMatchIndex; i++ {
						if clientCommand, isClientPending := n.clientCommandIndexMap[i]; isClientPending {
							n.logger.dlog3("respond to client", "log_index", i)
							clientCommand.replicated <- nil
							delete(n.clientCommandIndexMap, i)
						}
					}
				}
			}
		} else {
			if n.nextIndex[reply.Peer] > 1 {
				n.nextIndex[reply.Peer]--
			}
		}
	case clientCommand := <-n.clientCommandCh:
		n.logger.dlog2("handle client command")
		if n.state != Leader {
			clientCommand.replicated <- errors.New("not a leader")
			return
		}
		log := Log{
			Term:  n.currentTerm,
			Index: n.logStorage.len() + 1,
			Data:  clientCommand.data,
		}
		n.logStorage.append(log)
		n.clientCommandIndexMap[log.Index] = clientCommand
		n.sendAppendEntries(clientCommand.ctx)
		n.resetHeartbeatTimer()

	case appendEntries := <-n.appendEntriesRpcCh:
		n.logger.dlog2("handle AppendEntries call", "args", appendEntries.args)
		var reply AppendEntriesReply
		defer func() {
			appendEntries.replyCh <- reply
		}()
		reply.Peer = n.id
		reply.Term = n.currentTerm
		reply.Success = false
		if appendEntries.args.Term < n.currentTerm {
			return
		}
		reply.Success = true
		n.becomeFollower(appendEntries.args.Term, appendEntries.args.LeaderId)

		if n.logStorage.len() >= appendEntries.args.PrevLogIndex {
			if appendEntries.args.PrevLogIndex > 0 {
				logAtPrevLogIndex := n.logStorage.at(appendEntries.args.PrevLogIndex)
				if logAtPrevLogIndex.Term != appendEntries.args.PrevLogTerm {
					n.logger.dlog3("conflicting entry at PrevLogIndex", "args", appendEntries.args, "current_at_prev_log_index", logAtPrevLogIndex)
					reply.Success = false
					n.logStorage.clearFrom(appendEntries.args.PrevLogIndex)
					return
				}
			}
		} else {
			n.logger.dlog3("we have less logs than leader thinks", "args", appendEntries.args, "log_storage_len", n.logStorage.len())
			reply.Success = false
			return
		}

		if len(appendEntries.args.Entries) > 0 {
			for _, log := range appendEntries.args.Entries {
				if log.Index <= n.logStorage.len() {
					current := n.logStorage.at(log.Index)
					if current.Term == log.Term {
						n.logger.dlog3("log already replicated", "log", log)
					} else {
						n.logger.dlog3("conflicting entry", "log", log, "current_at_this_index", current)
						reply.Success = false
						n.logStorage.clearFrom(log.Index)
						return
					}
				} else {
					n.logStorage.append(log)
				}
			}
			n.logger.dlog3("append log entries", "args", appendEntries.args, "logs_count", n.logStorage.len())
		}

		if appendEntries.args.LeaderCommit > n.commitIndex && appendEntries.args.LeaderCommit <= n.logStorage.len() {
			n.logger.dlog("update commit index", "old_commit_index", n.commitIndex, "newCommitIndex", appendEntries.args.LeaderCommit, "log_storage", n.logStorage)
			newLogs := n.logStorage.slice(n.commitIndex+1, appendEntries.args.LeaderCommit+1)
			n.stateMachine.apply(newLogs...)
			n.commitIndex = appendEntries.args.LeaderCommit
		}

	case <-n.electionTimer.C:
		if n.state != Candidate {
			n.logger.Info("start election", "new_term", n.currentTerm+1)
		}
		n.startElection(ctx, n.requestVoteReplyCh)
		n.resetElectionTimer()
		n.heartbeatTimer.Stop()
	case reply := <-n.requestVoteReplyCh:
		n.logger.dlog2("handle RequestVote reply", "peer", reply.Peer, "reply", reply)
		if reply.Term > n.currentTerm {
			n.becomeFollower(reply.Term, reply.Peer)
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
				n.logger.Info("become leader")
				n.state = Leader
				for _, id := range n.peers {
					n.nextIndex[id] = n.logStorage.len() + 1
				}
				n.sendAppendEntries(ctx)
				n.resetHeartbeatTimer()
				n.electionTimer.Stop()
				n.electionSpan.SetAttributes(attribute.String("result", "success"))
				n.electionSpan.End()
				n.electionSpan = nil
			}
		}
	case requestVote := <-n.requestVoteRpcCh:
		n.logger.dlog2("handle RequestVote call", "peer", requestVote.args.CandidateId, "args", requestVote.args)
		var reply RequestVoteReply
		reply.VoteGranted = false
		if requestVote.args.Term > n.currentTerm || requestVote.args.Term == n.currentTerm && (n.votedFor == None || n.votedFor == requestVote.args.CandidateId) {
			n.becomeFollower(requestVote.args.Term, requestVote.args.CandidateId)
			n.votedFor = requestVote.args.CandidateId
			reply.VoteGranted = true
		}
		reply.Peer = n.id
		reply.Term = n.currentTerm
		requestVote.replyCh <- reply
	}
	return
}

func (n *Node) resetElectionTimer() {
	n.electionTimer.Reset(n.timeouts.Election + time.Duration(rand.Int63n(int64(n.timeouts.Election))))
}

func (n *Node) resetHeartbeatTimer() {
	n.heartbeatTimer.Reset(n.timeouts.Heartbeat)
}

func (n *Node) becomeFollower(term int, leaderId NodeId) {
	if n.state != Follower || n.currentTerm == 0 {
		n.logger.Info("become follower", "term", term, "leader", leaderId.String())
	}
	if n.electionSpan != nil {
		n.electionSpan.SetAttributes(attribute.String("result", "canceled"))
		n.electionSpan.End()
		n.electionSpan = nil
	}
	n.state = Follower
	n.currentTerm = term
	n.heartbeatTimer.Stop()
	n.resetElectionTimer()
}

func (n *Node) sendAppendEntries(ctx context.Context) {
	n.logger.dlog("send AppendEntries")
	for _, peer := range n.peers {
		args := AppendEntriesArgs{
			Term:         n.currentTerm,
			LeaderId:     n.id,
			LeaderCommit: n.commitIndex,
		}
		args.PrevLogIndex = n.nextIndex[peer] - 1
		if args.PrevLogIndex > 0 {
			args.PrevLogTerm = n.logStorage.at(args.PrevLogIndex).Term
		}
		lastLogIndex := n.logStorage.len()
		var entriesIndexFrom, entriesIndexTo int
		if lastLogIndex >= n.nextIndex[peer] {
			entriesIndexFrom = n.nextIndex[peer]
			entriesIndexTo = n.logStorage.len()
			entries := n.logStorage.slice(entriesIndexFrom, entriesIndexTo+1)
			args.Entries = make([]Log, len(entries))
			copy(args.Entries, entries)
		}
		n.logger.dlog3("send AppendEntries", "peer", peer, "args", args)
		ctx, span := Tracer.Start(ctx, "AppendEntries", trace.WithAttributes(
			attribute.String("node_id", n.Id().String()),
			attribute.String("peer_id", peer.String()),
		))
		go func() {
			reply, err := n.transport.AppendEntries(ctx, peer, args)
			if err != nil {
				EndSpanWithError(span, err)
				return
			}
			span.SetAttributes(attribute.Bool("success", reply.Success))
			reply.Peer = peer
			reply.entriesBounds.from = entriesIndexFrom
			reply.entriesBounds.to = entriesIndexTo
			n.appendEntriesReplyCh <- reply
			span.End()
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

	if n.electionSpan != nil {
		n.electionSpan.SetAttributes(attribute.String("result", "canceled"))
		n.electionSpan.End()
	}
	ctx, n.electionSpan = Tracer.Start(ctx, "election", trace.WithAttributes(
		attribute.String("node.id", n.Id().String()),
		attribute.Int("node.term", n.currentTerm),
	))

	for _, peer := range n.peers {
		n.logger.dlog3("send RequestVote", "peer", peer, "args", args)
		ctx, span := Tracer.Start(ctx, "RequestVote", trace.WithAttributes(
			attribute.String("node.id", n.Id().String()),
			attribute.String("peer_id", peer.String()),
		))
		ctx, cancel := context.WithTimeout(ctx, n.timeouts.Election*2)
		go func() {
			defer cancel()
			reply, err := n.transport.RequestVote(ctx, peer, args)
			if err != nil {
				EndSpanWithError(span, err)
				return
			}
			span.SetAttributes(attribute.Bool("success", reply.VoteGranted))
			reply.Peer = peer
			replyCh <- reply
			span.End()
		}()
	}
}

type SlogLogger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
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
