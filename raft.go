package raft

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var heartbeatPeriod = 50 * time.Millisecond
var electionTimeout = 150 * time.Millisecond
var electionTimeoutDelta = 150 * time.Millisecond
var transportTimeout = 10 * time.Second

func generateElectionTimeout() time.Duration {
	delta := time.Duration(rand.Int63n(int64(electionTimeoutDelta) * 2))
	return electionTimeout + delta
}

type requestVoteRpcCall struct {
	ctx    context.Context
	data   RequestVote
	result chan RequestVoteResult
}

type appendEntriesRpcCall struct {
	ctx    context.Context
	data   AppendEntries
	result chan AppendEntriesResult
}

type clientRequest struct {
	ctx    context.Context
	cmd    []byte
	client chan int
}

// TODO return errors from exposed functions as user may be dumb
type Node struct {
	Id               NodeId
	runCtx           context.Context
	transport        Transport
	mu               sync.Mutex
	votedFor         NodeId
	votesHave        int
	currentTerm      int
	state            State
	electionTimer    *time.Timer
	heartbeatTimer   *time.Timer
	shutdown         chan struct{}
	nodes            map[NodeId]string
	otherNodeIds     []NodeId
	nextIndex        map[NodeId]int
	matchIndex       map[NodeId]int
	StateMachine     *StateMachine
	verbose          bool
	requestVoteRpc   chan requestVoteRpcCall
	appendEntriesRpc chan appendEntriesRpcCall
	commitCh         chan int
	clientRequestCh  chan clientRequest
	logger           *slog.Logger
	eventHandlers    []*EventHandler
}

func (n *Node) Verbose() *Node {
	n.verbose = true
	return n
}

func (n *Node) startSpan(ctx context.Context, name string) (context.Context, trace.Span) {
	return otel.Tracer("raft").Start(ctx, name, trace.WithAttributes(attribute.String("nodeId", n.Id.String())))
}

func NewNode(id NodeId, nodes map[NodeId]string, transport Transport, storage StateStorage, logger *slog.Logger) *Node {
	otherNodeIds := []NodeId{}
	for otherId := range nodes {
		if id != otherId {
			otherNodeIds = append(otherNodeIds, otherId)
		}
	}
	return &Node{
		Id:               id,
		transport:        transport,
		nodes:            nodes,
		state:            Dead,
		otherNodeIds:     otherNodeIds,
		StateMachine:     NewStateMachine(storage),
		clientRequestCh:  make(chan clientRequest),
		requestVoteRpc:   make(chan requestVoteRpcCall),
		appendEntriesRpc: make(chan appendEntriesRpcCall),
		nextIndex:        make(map[NodeId]int),
		matchIndex:       make(map[NodeId]int),
		logger:           logger.With("node", id.String()),
	}
}

func (n *Node) RegisterEventHandler(handler func(h *EventHandler, event any)) *EventHandler {
	n.mu.Lock()
	defer n.mu.Unlock()
	h := &EventHandler{handler, true}
	n.eventHandlers = append(n.eventHandlers, h)
	return h
}

func (n *Node) CurrentTerm() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm
}

func (n *Node) State() State {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state
}

func (n *Node) incrementTerm() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.currentTerm++
	return n.currentTerm
}

func (n *Node) setCurrentTerm(term int) {
	n.mu.Lock()
	was := n.currentTerm
	n.currentTerm = term
	n.mu.Unlock()
	if was != term {
		n.dispatchEvent(EventTerm{term})
	}
}

func (n *Node) setState(s State) State {
	n.mu.Lock()
	defer n.mu.Unlock()
	was := n.state
	n.state = s
	return was
}

func (n *Node) getVotedFor() NodeId {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.votedFor
}

func (n *Node) setVotedFor(id NodeId) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.votedFor = id
}

func (n *Node) incrementVotesHave() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.votesHave++
	return n.votesHave
}

func (n *Node) setVotesHave(value int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.votesHave = value
}

func (n *Node) Shutdown(ctx context.Context) {
	if n.State() == Dead {
		return
	}
	n.logger.InfoContext(ctx, "shutting down")
	n.shutdown <- struct{}{}
	close(n.shutdown)
	if err := n.transport.ShutdownServer(ctx); err != nil {
		n.logger.ErrorContext(ctx, "could not shutdown HTTP server", "error", err.Error())
	}
}

// Blocks until majority of the cluster agrees on a command
func (n *Node) ClientCommand(ctx context.Context, command []byte) {
	ctx, span := n.startSpan(ctx, "client command")
	defer span.End()
	n.logger.InfoContext(ctx, "incoming client command", "bytes", len(command))
	ur := clientRequest{
		ctx,
		command,
		make(chan int),
	}
	select {
	case n.clientRequestCh <- ur:
		<-ur.client
	case <-ctx.Done():
	}
}

func (n *Node) RequestVote(ctx context.Context, requestVote RequestVote) (RequestVoteResult, error) {
	responseCh := make(chan RequestVoteResult)
	select {
	case <-n.runCtx.Done():
		return RequestVoteResult{}, n.runCtx.Err()
	case <-ctx.Done():
		return RequestVoteResult{}, ctx.Err()
	case n.requestVoteRpc <- requestVoteRpcCall{ctx, requestVote, responseCh}:
	}
	select {
	case <-n.runCtx.Done():
		return RequestVoteResult{}, n.runCtx.Err()
	case r := <-responseCh:
		return r, nil
	}
}

func (n *Node) AppendEntries(ctx context.Context, appendEntries AppendEntries) (AppendEntriesResult, error) {
	responseCh := make(chan AppendEntriesResult)
	select {
	case <-n.runCtx.Done():
		return AppendEntriesResult{}, n.runCtx.Err()
	case <-ctx.Done():
		return AppendEntriesResult{}, ctx.Err()
	case n.appendEntriesRpc <- appendEntriesRpcCall{ctx, appendEntries, responseCh}:
	}
	select {
	case <-n.runCtx.Done():
		return AppendEntriesResult{}, n.runCtx.Err()
	case r := <-responseCh:
		return r, nil
	}
}

func (n *Node) Run(ctx context.Context) error {
	if n.State() != Dead {
		n.logger.InfoContext(ctx, "node is already running")
		return nil
	}

	if err := n.transport.Serve(n); err != nil {
		return fmt.Errorf("could not run node server: %w", err)
	}
	n.mu.Lock()
	n.state = Follower
	n.shutdown = make(chan struct{})
	n.votedFor = EmptyId

	n.electionTimer = time.NewTimer(generateElectionTimeout())
	n.heartbeatTimer = time.NewTimer(heartbeatPeriod)
	n.heartbeatTimer.Stop()
	defer n.electionTimer.Stop()
	defer n.heartbeatTimer.Stop()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	n.runCtx = ctx

	n.mu.Unlock()
	n.logger.InfoContext(ctx, "starting event loop")
	n.eventLoop(n.runCtx)
	n.logger.InfoContext(ctx, "stopped event loop")
	n.setState(Dead)
	return nil
}

type appendEntriesFollowerResult struct {
	request *AppendEntries
	result  AppendEntriesResult
	client  chan int
	id      NodeId
}

func (n *Node) eventLoop(ctx context.Context) {
	requestVoteResponse := make(chan RequestVoteResult)
	defer close(requestVoteResponse)
	appendEntriesResponse := make(chan appendEntriesFollowerResult)
	// TODO uncomment
	// defer close(appendEntriesResponse)

	// defer n.electionTimer.Stop()
	// defer n.heartbeatTimer.Stop()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
eventLoop:
	for {
		select {
		case <-n.shutdown:
			n.logger.InfoContext(ctx, "shut down event loop")
			break eventLoop

		case appendEntries := <-n.appendEntriesRpc:
			appendEntries.result <- n.appendEntriesRPC(appendEntries.ctx, appendEntries.data)
		case requestVote := <-n.requestVoteRpc:
			requestVote.result <- n.requestVoteRPC(requestVote.ctx, requestVote.data)
		case <-n.electionTimer.C:
			electionCtx, span := n.startSpan(ctx, "election")
			n.startElection(electionCtx, requestVoteResponse)
			span.End()
		case response := <-requestVoteResponse:
			n.onRequestVoteResponse(ctx, response)

		case <-n.heartbeatTimer.C:
			heartbeatCtx, span := n.startSpan(ctx, "heartbeat")
			n.sendHeartbeats(heartbeatCtx, appendEntriesResponse)
			span.End()
		case appendEntriesResult := <-appendEntriesResponse:
			n.onAppendEntriesResponse(ctx, appendEntriesResult, appendEntriesResponse)
		case req := <-n.clientRequestCh:
			n.onClientCommand(req, appendEntriesResponse)
		}

		switch n.State() {
		case Follower:
			n.electionTimer.Reset(generateElectionTimeout())
		case Candidate:
			n.electionTimer.Reset(generateElectionTimeout())
		case Leader:
			n.heartbeatTimer.Reset(heartbeatPeriod)
		}
	}
}

// maybe return error to a client?
func (n *Node) onClientCommand(req clientRequest, appendEntriesResponse chan<- appendEntriesFollowerResult) {
	ctx := req.ctx
	if state := n.State(); state != Leader {
		n.logger.WarnContext(ctx, "got client request while not being leader", "state", state)
		req.client <- 1
		return
	}
	n.StateMachine.AppendLogs(Entry{req.cmd, n.CurrentTerm()})
	for _, id := range n.otherNodeIds {
		go func() {
			appendEntries := n.makeAppendEntries(id)
			timeoutctx, cancel := context.WithTimeout(ctx, transportTimeout)
			defer cancel()
			result, err := n.transport.IssueAppendEntries(timeoutctx, appendEntries, id)
			if err != nil {
				n.logger.ErrorContext(ctx, "could not issue AppendEntries", "dest", id, "error", err.Error())
			} else {
				appendEntriesResponse <- appendEntriesFollowerResult{&appendEntries, result, req.client, id}
			}
		}()
	}
}

func (n *Node) onAppendEntriesResponse(ctx context.Context, appendEntriesResult appendEntriesFollowerResult, appendEntriesResponse chan<- appendEntriesFollowerResult) {
	if state := n.State(); state != Leader {
		n.logger.WarnContext(ctx, "got AppendEntriesRPCResult while not being leader", "state", state)
		return
	}
	if appendEntriesResult.result.Term > n.CurrentTerm() {
		n.setCurrentTerm(appendEntriesResult.result.Term)
		n.becomeFollower(ctx)
		return
	}
	if appendEntriesResult.result.Success {
		n.nextIndex[appendEntriesResult.id] = appendEntriesResult.request.LogIndex + 1
		n.matchIndex[appendEntriesResult.id] = appendEntriesResult.request.LogIndex
		if len(appendEntriesResult.request.Entries) > 0 {
			savedCommitIndex := n.StateMachine.CommitIndex()
			newCommitIndex := savedCommitIndex
			for i := n.StateMachine.CommitIndex() + 1; i < n.StateMachine.Len(); i++ {
				if n.StateMachine.MustAt(i).Term == n.CurrentTerm() {
					matchCount := 1
					for _, peerId := range n.otherNodeIds {
						if n.matchIndex[peerId] >= i {
							matchCount++
						}
					}
					if matchCount*2 > len(n.otherNodeIds)+1 {
						newCommitIndex = i
					}
				}
			}
			if newCommitIndex > savedCommitIndex {
				n.logger.InfoContext(ctx, "update commit index", "value", newCommitIndex, "previous", savedCommitIndex)
				n.StateMachine.SetCommitIndex(newCommitIndex)
				n.StateMachine.Apply(newCommitIndex)
				appendEntriesResult.client <- newCommitIndex
				n.dispatchEvent(EventCommit{newCommitIndex})
			}
		}
	} else {
		n.mu.Lock()
		n.nextIndex[appendEntriesResult.id]--
		n.mu.Unlock()
		go func() {
			appendEntries := n.makeAppendEntries(appendEntriesResult.id)
			timeoutctx, cancel := context.WithTimeout(ctx, transportTimeout)
			defer cancel()
			result, err := n.transport.IssueAppendEntries(timeoutctx, appendEntries, appendEntriesResult.id)
			if err != nil {
				n.logger.ErrorContext(ctx, "could not reissue AppendEntries", "dest", appendEntriesResult.id, "error", err.Error())
			} else {
				select {
				case <-ctx.Done():
				case appendEntriesResponse <- appendEntriesFollowerResult{&appendEntries, result, appendEntriesResult.client, appendEntriesResult.id}:
				}
			}
		}()
	}
}

func (n *Node) sendHeartbeats(ctx context.Context, appendEntriesResponse chan<- appendEntriesFollowerResult) {
	if state := n.State(); state != Leader {
		n.logger.WarnContext(ctx, "got heartbeatTimer tick while not being a leader", "state", state)
		return
	}
	for _, id := range n.otherNodeIds {
		go func() {
			appendEntries := n.makeAppendEntries(id)
			timeoutctx, cancel := context.WithTimeout(ctx, transportTimeout)
			defer cancel()
			result, err := n.transport.IssueAppendEntries(timeoutctx, n.makeAppendEntries(id), id)
			if err != nil {
				n.logger.DebugContext(ctx, "could not issue heartbeat", "dest", id, "error", err.Error())
			} else {
				select {
				case <-ctx.Done():
				case appendEntriesResponse <- appendEntriesFollowerResult{&appendEntries, result, nil, id}:
				}
			}
		}()
	}
}

func (n *Node) onRequestVoteResponse(ctx context.Context, response RequestVoteResult) {
	if state := n.State(); state != Candidate {
		n.logger.WarnContext(ctx, "got RequestVoteRPCResult while not being a Candidate", "state", state)
		return
	}
	if response.Term > n.CurrentTerm() {
		n.setCurrentTerm(response.Term)
		n.becomeFollower(ctx)
		return
	}
	if response.VoteGranted {
		// TODO: check vote source
		votes := n.incrementVotesHave()
		if votes*2 > len(n.nodes) {
			n.becomeLeader(ctx)
		}
	}
}

func (n *Node) startElection(ctx context.Context, requestVoteResponse chan<- RequestVoteResult) {
	n.becomeCandidate(ctx)
	for _, id := range n.otherNodeIds {
		go func() {
			n.logger.DebugContext(ctx, "issuing RequestVote", "dest", id)
			timeoutctx, cancel := context.WithTimeout(ctx, transportTimeout)
			defer cancel()
			result, err := n.transport.IssueRequestVote(timeoutctx, RequestVote{
				Term:        n.CurrentTerm(),
				CandidateId: n.Id,
			}, id)
			if err != nil {
				n.logger.DebugContext(ctx, "could not issue RequestVote", "dest", id, "error", err.Error())
			} else {
				select {
				case <-ctx.Done():
				case requestVoteResponse <- result:
				}
			}
		}()
	}
}

// Receiver implementation:
// 1. Reply false if term < currentTerm (§5.1)
// 2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2,§5.4)
func (n *Node) requestVoteRPC(ctx context.Context, requestVote RequestVote) RequestVoteResult {
	n.logger.DebugContext(ctx, "got RequestVoteRPC call", "from", requestVote.CandidateId)
	response := RequestVoteResult{}
	lastLog, lastIndex, ok := n.StateMachine.Last()
	candidateUpToDate := !ok || lastIndex >= requestVote.LastLogIndex && lastLog.Term >= requestVote.LastLogTerm
	if requestVote.Term > n.CurrentTerm() || (n.getVotedFor() == EmptyId && candidateUpToDate) {
		n.setCurrentTerm(requestVote.Term)
		n.becomeFollower(ctx)
		n.setVotedFor(requestVote.CandidateId)
		response.VoteGranted = true
	}
	response.Term = n.CurrentTerm()
	return response
}

// Receiver implementation:
// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. TODO If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
func (n *Node) appendEntriesRPC(ctx context.Context, appendEntries AppendEntries) AppendEntriesResult {
	response := AppendEntriesResult{Term: n.CurrentTerm()}
	if appendEntries.Term < n.CurrentTerm() {
		return response
	}
	if len(appendEntries.Entries) > 0 {
		if appendEntries.PrevLogIndex > -1 {
			lastEntry, ok := n.StateMachine.At(appendEntries.PrevLogIndex)
			if !ok {
				return response
			}
			if lastEntry.Term != appendEntries.PrevLogTerm {
				n.StateMachine.DeleteFrom(appendEntries.PrevLogIndex)
				n.logger.InfoContext(ctx, "delete irrelevant logs", "fromIndex", appendEntries.PrevLogIndex)
				return response
			}
		} else if nlogs := n.StateMachine.Len(); nlogs > 0 {
			n.StateMachine.DeleteFrom(0)
			n.logger.InfoContext(ctx, "delete irrelevant logs", "fromIndex", appendEntries.PrevLogIndex)
			return response
		}
		logs := n.StateMachine.AppendLogs(appendEntries.Entries...)
		n.logger.InfoContext(ctx, "replicated leader logs", "countNewEntries", len(appendEntries.Entries), "countTotalLogs", logs)
	}
	n.setCurrentTerm(appendEntries.Term)
	n.becomeFollower(ctx)
	if appendEntries.LeaderCommit > n.StateMachine.CommitIndex() {
		actualCommit := appendEntries.LeaderCommit
		n.StateMachine.Apply(actualCommit)
		n.StateMachine.SetCommitIndex(actualCommit)
		n.logger.InfoContext(ctx, "Applied new commit index", "value", actualCommit)
		n.dispatchEvent(EventCommit{actualCommit})
	}
	response.Success = true
	return response
}

func (n *Node) makeAppendEntries(peer NodeId) AppendEntries {
	n.mu.Lock()
	nextIndex := n.nextIndex[peer]
	n.mu.Unlock()
	entries := make([]Entry, 0)
	prevLogIndex := nextIndex - 1
	prevLogTerm := 0
	if prevLogIndex < n.StateMachine.Len() {
		entries = n.StateMachine.NextEntriesForFollower(nextIndex)
		if prevLogIndex > 0 {
			prevLogTerm = n.StateMachine.MustAt(prevLogIndex).Term
		}
	}
	return AppendEntries{
		Term:         n.CurrentTerm(),
		LeaderId:     n.Id,
		LeaderCommit: n.StateMachine.CommitIndex(),
		Entries:      entries,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		LogIndex:     n.StateMachine.Len() - 1,
	}
}

func (n *Node) becomeCandidate(ctx context.Context) {
	if n.setState(Candidate) != Candidate {
		n.logger.InfoContext(ctx, "switch state", "value", Candidate)
	}
	term := n.incrementTerm()
	n.setVotedFor(n.Id)
	n.setVotesHave(1)
	n.logger.InfoContext(ctx, "election", "term", term)
	n.dispatchEvent(EventBecomeCandidate{})
}

func (n *Node) becomeFollower(ctx context.Context) {
	if n.setState(Follower) != Follower {
		n.logger.InfoContext(ctx, "switch state", "value", Follower)
	}
	n.electionTimer.Reset(generateElectionTimeout())
	n.heartbeatTimer.Stop()
	n.dispatchEvent(EventBecomeFollower{})
}

func (n *Node) becomeLeader(ctx context.Context) {
	if n.setState(Leader) != Leader {
		n.logger.InfoContext(ctx, "switch state", "value", Leader)
	}
	n.mu.Lock()
	for _, id := range n.otherNodeIds {
		n.nextIndex[id] = n.StateMachine.LastApplied() + 1
		n.matchIndex[id] = 0
	}
	n.mu.Unlock()
	n.electionTimer.Stop()
	n.heartbeatTimer.Reset(heartbeatPeriod)
	n.dispatchEvent(EventBecomeLeader{})
}

func (n *Node) dispatchEvent(event any) {
	if !n.mu.TryLock() {
		panic("Node.dispatchEvent with active lock")
	}
	handlers := make([]*EventHandler, len(n.eventHandlers))
	copy(handlers, n.eventHandlers)
	n.mu.Unlock()
	for i := range handlers {
		if handlers[i].active {
			handlers[i].f(handlers[i], event)
		}
	}
}

func ParseNodesFlag(nodesStr string) (map[NodeId]string, error) {
	nodes := make(map[NodeId]string, 3)
	nodeParts := strings.Split(nodesStr, ";")
	for i := range nodeParts {
		idHostAndPort := strings.Split(nodeParts[i], ":")
		if len(idHostAndPort) != 3 {
			return nil, fmt.Errorf("invalid peer configuration: %s", nodeParts[i])
		}
		nodes[NodeId(idHostAndPort[0])] = idHostAndPort[1] + ":" + idHostAndPort[2]
	}
	return nodes, nil
}
