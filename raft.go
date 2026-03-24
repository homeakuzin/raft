package raft

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

var heartbeatPeriod = 50 * time.Millisecond
var electionTimeout = 150 * time.Millisecond
var electionTimeoutDelta = 150 * time.Millisecond

func generateElectionTimeout() time.Duration {
	delta := time.Duration(rand.Int63n(int64(electionTimeoutDelta) * 2))
	return electionTimeout + delta
}

type requestVoteRpcCall struct {
	data   RequestVote
	result chan RequestVoteResult
}

type appendEntriesRpcCall struct {
	data   AppendEntries
	result chan AppendEntriesResult
}

type clientRequest struct {
	cmd    []byte
	client chan int
}

// TODO return errors from exposed functions as user may be dumb
type Node struct {
	Id               NodeId
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
	logger           *log.Logger
	debug            bool
}

func (n *Node) Verbose() *Node {
	n.verbose = true
	return n
}

func NewNode(id NodeId, nodes map[NodeId]string, transport Transport, storage StateStorage) *Node {
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
		logger:           log.New(log.Writer(), log.Prefix(), log.Flags()),
	}
}

func (n *Node) LogPrefixId() *Node {
	n.logger.SetPrefix(fmt.Sprintf("[%s] ", n.Id))
	return n
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
	defer n.mu.Unlock()
	n.currentTerm = term
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
	n.logger.Print("shutting down")
	n.shutdown <- struct{}{}
	close(n.shutdown)
	if err := n.transport.Shutdown(ctx); err != nil {
		n.logger.Printf("could not shutdown HTTP server: %s", err.Error())
	}
}

// Blocks until majority of the cluster agrees on a command
func (n *Node) ClientCommand(ctx context.Context, command []byte) {
	n.logger.Printf("incoming command (%d bytes)", len(command))
	ur := clientRequest{
		command,
		make(chan int),
	}
	select {
	case n.clientRequestCh <- ur:
		<-ur.client
	case <-ctx.Done():
	}
}

func (n *Node) RequestVote(requestVote RequestVote) RequestVoteResult {
	responseCh := make(chan RequestVoteResult)
	n.requestVoteRpc <- requestVoteRpcCall{requestVote, responseCh}
	return <-responseCh
}

func (n *Node) AppendEntries(appendEntries AppendEntries) AppendEntriesResult {
	responseCh := make(chan AppendEntriesResult)
	n.appendEntriesRpc <- appendEntriesRpcCall{appendEntries, responseCh}
	return <-responseCh
}

func (n *Node) Run() error {
	if n.State() != Dead {
		n.logger.Print("already running")
		return nil
	}

	if err := n.transport.Serve(n); err != nil {
		n.logger.Printf("could not run node server: %s", err.Error())
		return err
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

	n.mu.Unlock()
	n.logger.Print("starting event loop")
	n.eventLoop()
	n.logger.Print("stopped event loop")
	n.setState(Dead)
	return nil
}

type appendEntriesFollowerResult struct {
	request *AppendEntries
	result  AppendEntriesResult
	client  chan int
	id      NodeId
}

func (n *Node) eventLoop() {
	requestVoteResponse := make(chan RequestVoteResult)
	defer close(requestVoteResponse)
	appendEntriesResponse := make(chan appendEntriesFollowerResult)
	defer close(appendEntriesResponse)

	// defer n.electionTimer.Stop()
	// defer n.heartbeatTimer.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
eventLoop:
	for {
		select {
		case <-n.shutdown:
			n.logger.Print("shut down event loop")
			// cancel()
			break eventLoop

		case appendEntries := <-n.appendEntriesRpc:
			appendEntries.result <- n.appendEntriecRPC(appendEntries.data)
		case requestVote := <-n.requestVoteRpc:
			requestVote.result <- n.requestVoteRPC(requestVote.data)
		case <-n.electionTimer.C:
			n.startElection(ctx, requestVoteResponse)
		case response := <-requestVoteResponse:
			n.onRequestVoteResponse(response)

		case <-n.heartbeatTimer.C:
			n.sendHeartbeats(ctx, appendEntriesResponse)
		case appendEntriesResult := <-appendEntriesResponse:
			n.onAppendEntriesResponse(ctx, appendEntriesResult, appendEntriesResponse)
		case req := <-n.clientRequestCh:
			n.onClientCommand(ctx, req, appendEntriesResponse)
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
func (n *Node) onClientCommand(ctx context.Context, req clientRequest, appendEntriesResponse chan<- appendEntriesFollowerResult) {
	if state := n.State(); state != Leader {
		n.logger.Printf("Warning: got client request in state %s", state)
		req.client <- 1
		return
	}
	n.StateMachine.AppendLogs(Entry{req.cmd, n.CurrentTerm()})
	for _, id := range n.otherNodeIds {
		go func() {
			appendEntries := n.makeAppendEntries(id)
			result, err := n.transport.IssueAppendEntries(ctx, appendEntries, id)
			if err != nil {
				n.logger.Printf("could not issue AppendEntries to %s: %s", id, err.Error())
			} else {
				appendEntriesResponse <- appendEntriesFollowerResult{&appendEntries, result, req.client, id}
			}
		}()
	}
}

func (n *Node) onAppendEntriesResponse(ctx context.Context, appendEntriesResult appendEntriesFollowerResult, appendEntriesResponse chan<- appendEntriesFollowerResult) {
	if state := n.State(); state != Leader {
		n.logger.Printf("Warning: got AppendEntriesRPCResult in state %s", state)
		return
	}
	if appendEntriesResult.result.Term > n.CurrentTerm() {
		n.setCurrentTerm(appendEntriesResult.result.Term)
		n.becomeFollower()
		return
	}
	if appendEntriesResult.result.Success {
		n.nextIndex[appendEntriesResult.id] = appendEntriesResult.request.LogIndex + 1
		n.matchIndex[appendEntriesResult.id] = appendEntriesResult.request.LogIndex
		if len(appendEntriesResult.request.Entries) > 0 {
			savedCommitIndex := n.StateMachine.CommitIndex()
			for i := n.StateMachine.CommitIndex() + 1; i < n.StateMachine.Len(); i++ {
				if n.StateMachine.MustAt(i).Term == n.CurrentTerm() {
					matchCount := 1
					for _, peerId := range n.otherNodeIds {
						if n.matchIndex[peerId] >= i {
							matchCount++
						}
					}
					if matchCount*2 > len(n.otherNodeIds)+1 {
						n.StateMachine.SetCommitIndex(i)
					}
				}
			}
			if n.StateMachine.CommitIndex() > savedCommitIndex {
				n.logger.Printf("set CommitIndex = %d", n.StateMachine.CommitIndex())
				n.StateMachine.Apply(n.StateMachine.CommitIndex())
				appendEntriesResult.client <- n.StateMachine.CommitIndex()
			}
		}
	} else {
		n.mu.Lock()
		n.nextIndex[appendEntriesResult.id]--
		n.mu.Unlock()
		go func() {
			appendEntries := n.makeAppendEntries(appendEntriesResult.id)
			result, err := n.transport.IssueAppendEntries(ctx, appendEntries, appendEntriesResult.id)
			if err != nil {
				n.logger.Printf("could not reissue AppendEntries to %s: %s", appendEntriesResult.id, err.Error())
			} else {
				appendEntriesResponse <- appendEntriesFollowerResult{&appendEntries, result, appendEntriesResult.client, appendEntriesResult.id}
			}
		}()
	}
}

func (n *Node) sendHeartbeats(ctx context.Context, appendEntriesResponse chan<- appendEntriesFollowerResult) {
	if state := n.State(); state != Leader {
		n.logger.Printf("Warning: got heartbeatTimer tick in state %s", state)
		return
	}
	for _, id := range n.otherNodeIds {
		go func() {
			appendEntries := n.makeAppendEntries(id)
			result, err := n.transport.IssueAppendEntries(ctx, n.makeAppendEntries(id), id)
			if err != nil {
				n.dlog("could not issue heartbeat to %s: %s", id, err.Error())
			} else {
				appendEntriesResponse <- appendEntriesFollowerResult{&appendEntries, result, nil, id}
			}
		}()
	}
}

func (n *Node) onRequestVoteResponse(response RequestVoteResult) {
	if state := n.State(); state != Candidate {
		n.logger.Printf("Warning: got RequestVoteRPCResult in state %s", state)
		return
	}
	if response.Term > n.CurrentTerm() {
		n.setCurrentTerm(response.Term)
		n.becomeFollower()
		return
	}
	if response.VoteGranted {
		// TODO: check vote source
		votes := n.incrementVotesHave()
		if votes*2 > len(n.nodes) {
			n.becomeLeader()
		}
	}
}

func (n *Node) startElection(ctx context.Context, requestVoteResponse chan<- RequestVoteResult) {
	n.becomeCandidate()
	for _, id := range n.otherNodeIds {
		go func() {
			n.dlog("issuing RequestVote to %s", id)
			result, err := n.transport.IssueRequestVote(ctx, RequestVote{
				Term:        n.CurrentTerm(),
				CandidateId: n.Id,
			}, id)
			if err != nil {
				n.dlog("could not issue RequestVote to %s: %s", id, err.Error())
			} else {
				requestVoteResponse <- result
			}
		}()
	}
}

// Receiver implementation:
// 1. Reply false if term < currentTerm (§5.1)
// 2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2,§5.4)
func (n *Node) requestVoteRPC(requestVote RequestVote) RequestVoteResult {
	n.dlog("RequestVoteRPC from %s", requestVote.CandidateId)
	response := RequestVoteResult{}
	lastLog, lastIndex, ok := n.StateMachine.Last()
	candidateUpToDate := !ok || lastIndex >= requestVote.LastLogIndex && lastLog.Term >= requestVote.LastLogTerm
	if requestVote.Term > n.CurrentTerm() || (n.getVotedFor() == EmptyId && candidateUpToDate) {
		n.setCurrentTerm(requestVote.Term)
		n.becomeFollower()
		n.setVotedFor(requestVote.CandidateId)
		response.VoteGranted = true
	}
	response.Term = n.CurrentTerm()
	return response
}

func (n *Node) Debug() {
	n.debug = true
}

// Receiver implementation:
// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. TODO If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
func (n *Node) appendEntriecRPC(appendEntries AppendEntries) AppendEntriesResult {
	response := AppendEntriesResult{Term: n.CurrentTerm()}
	if appendEntries.Term < n.CurrentTerm() {
		return response
	}
	if len(appendEntries.Entries) > 0 {
		n.logger.Printf("%v", appendEntries)
		if appendEntries.PrevLogIndex > -1 {
			lastEntry, ok := n.StateMachine.At(appendEntries.PrevLogIndex)
			if !ok {
				return response
			}
			if lastEntry.Term != appendEntries.PrevLogTerm {
				n.StateMachine.DeleteFrom(appendEntries.PrevLogIndex)
				return response
			}
		} else if nlogs := n.StateMachine.Len(); nlogs > 0 {
			n.StateMachine.DeleteFrom(0)
			n.logger.Printf("leader thought we had no logs. huh. we have %d of them", nlogs)
			return response
		}
		logs := n.StateMachine.AppendLogs(appendEntries.Entries...)
		n.logger.Printf("replicated %d leader logs: %d logs now", len(appendEntries.Entries), logs)
	}
	n.setCurrentTerm(appendEntries.Term)
	n.becomeFollower()
	if appendEntries.LeaderCommit > n.StateMachine.CommitIndex() {
		actualCommit := appendEntries.LeaderCommit
		n.StateMachine.Apply(actualCommit)
		n.StateMachine.SetCommitIndex(actualCommit)
		n.logger.Printf("Applied commit index %d", actualCommit)
	}
	response.Success = true
	n.logger.Printf("respond to %v", appendEntries)
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

func (n *Node) becomeCandidate() {
	if n.setState(Candidate) != Candidate {
		n.logger.Printf("Turning to a Candidate")
	}
	term := n.incrementTerm()
	n.setVotedFor(n.Id)
	n.setVotesHave(1)
	n.logger.Printf("Term %d: election", term)
}

func (n *Node) becomeFollower() {
	if n.setState(Follower) != Follower {
		n.logger.Printf("Turning to a Follower")
	}
	n.electionTimer.Reset(generateElectionTimeout())
	n.heartbeatTimer.Stop()
}

func (n *Node) becomeLeader() {
	if n.setState(Leader) != Leader {
		n.logger.Printf("Turning to a Leader")
	}
	n.mu.Lock()
	for _, id := range n.otherNodeIds {
		n.nextIndex[id] = n.StateMachine.LastApplied() + 1
		n.matchIndex[id] = 0
	}
	n.mu.Unlock()
	n.electionTimer.Stop()
	n.heartbeatTimer.Reset(heartbeatPeriod)
}

func (n *Node) dlog(format string, v ...any) {
	if n.verbose {
		n.logger.Printf(format, v...)
	}
}
