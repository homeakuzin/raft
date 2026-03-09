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
	cmd      []byte
	commited chan int
}

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
	if err := n.transport.Shutdown(ctx); err != nil {
		n.logger.Printf("could not shutdown HTTP server: %s", err.Error())
	}
	n.shutdown <- struct{}{}
	close(n.shutdown)
}

func (n *Node) ClientCommand(ctx context.Context, command []byte) {
	ur := clientRequest{
		command,
		make(chan int),
	}
	select {
	case n.clientRequestCh <- ur:
		<-ur.commited
	case <-ctx.Done():
	}
}

func (n *Node) Run() {
	if n.State() != Dead {
		n.logger.Print("already running")
		return
	}
	if err := n.transport.Serve(n); err != nil {
		n.logger.Printf("could not run node server: %s", err.Error())
		n.Shutdown(context.Background())
	}
	n.mu.Lock()
	n.state = Follower
	n.shutdown = make(chan struct{})
	n.votedFor = EmptyId

	n.electionTimer = time.NewTimer(generateElectionTimeout())
	defer n.electionTimer.Stop()
	n.heartbeatTimer = time.NewTimer(heartbeatPeriod)
	defer n.heartbeatTimer.Stop()
	n.heartbeatTimer.Stop()

	n.mu.Unlock()
	n.logger.Print("starting event loop")
	n.eventLoop()
	n.logger.Print("stopped event loop")
	n.setState(Dead)
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
	heartbeatResponse := make(chan AppendEntriesResult)
	defer close(heartbeatResponse)
	appendEntriesResponse := make(chan appendEntriesFollowerResult)
	defer close(appendEntriesResponse)

eventLoop:
	for {
		select {
		case <-n.shutdown:
			n.logger.Print("shut down event loop")
			break eventLoop
		case appendEntries := <-n.appendEntriesRpc:
			// Receiver implementation:
			// 1. Reply false if term < currentTerm (§5.1)
			// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
			// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
			// 4. Append any new entries not already in the log
			// TODO 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
			response := AppendEntriesResult{}
			if appendEntries.data.Term >= n.CurrentTerm() {
				n.setCurrentTerm(appendEntries.data.Term)
				n.becomeFollower()
				// response.Success = true // TODO?
			}
			if appendEntries.data.LeaderCommit > n.StateMachine.CommitIndex() {
				newCommit := appendEntries.data.LeaderCommit
				n.StateMachine.Apply(newCommit)
				n.StateMachine.SetCommitIndex(newCommit)
				n.logger.Printf("Applied commit index %d", newCommit)
			}
			if len(appendEntries.data.Entries) > 0 {
				if appendEntries.data.PrevLogIndex > 0 {
					lastEntry, ok := n.StateMachine.At(appendEntries.data.PrevLogIndex)
					if ok {
						if lastEntry.Term == appendEntries.data.PrevLogTerm {
							response.Success = true
							logs := n.StateMachine.AppendLogs(appendEntries.data.Entries...)
							// TODO this may repeat again and again
							n.logger.Printf("Replicated %d Leader entries: %d logs now", len(appendEntries.data.Entries), logs)
						} else {
							n.StateMachine.DeleteFrom(appendEntries.data.PrevLogIndex)
						}
					}
				} else {
					response.Success = true
					logs := n.StateMachine.AppendLogs(appendEntries.data.Entries...)
					// TODO this may repeat again and again
					n.logger.Printf("Replicated first %d Leader entries: %d logs now", len(appendEntries.data.Entries), logs)
				}
			}
			response.Term = n.CurrentTerm()
			appendEntries.result <- response
		case requestVote := <-n.requestVoteRpc:
			// Receiver implementation:
			// 1. Reply false if term < currentTerm (§5.1)
			// 2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2,§5.4)
			n.dlog("RequestVoteRPC from %s", requestVote.data.CandidateId)
			response := RequestVoteResult{}
			lastLog, lastIndex, ok := n.StateMachine.Last()
			candidateUpToDate := !ok || lastIndex >= requestVote.data.LastLogIndex && lastLog.Term >= requestVote.data.LastLogTerm
			if requestVote.data.Term > n.CurrentTerm() || (n.getVotedFor() == EmptyId && candidateUpToDate) {
				n.setCurrentTerm(requestVote.data.Term)
				n.becomeFollower()
				n.setVotedFor(requestVote.data.CandidateId)
				response.VoteGranted = true
			}
			response.Term = n.CurrentTerm()
			requestVote.result <- response
		case <-n.electionTimer.C:
			n.becomeCandidate()
			for _, id := range n.otherNodeIds {
				go func() {
					n.dlog("issuing RequestVote to %s", id)
					result, err := n.transport.IssueRequestVote(context.Background(), RequestVote{
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
		case response := <-requestVoteResponse:
			if state := n.State(); state != Candidate {
				n.logger.Printf("Warning: got RequestVoteRPCResult in state %s", state)
				break
			}
			if response.Term > n.CurrentTerm() {
				n.setCurrentTerm(response.Term)
				n.becomeFollower()
				break
			}
			if response.VoteGranted {
				// TODO: check vote source
				votes := n.incrementVotesHave()
				if votes*2 > len(n.nodes) {
					n.becomeLeader()
				}
			}

		case <-n.heartbeatTimer.C:
			if state := n.State(); state != Leader {
				n.logger.Printf("Warning: got heartbeatTimer tick in state %s", state)
				break
			}
			appendEntriesCtx := context.Background()
			for _, id := range n.otherNodeIds {
				go func() {
					result, err := n.transport.IssueAppendEntries(appendEntriesCtx, n.makeAppendEntries(id), id)
					if err != nil {
						n.dlog("could not issue heartbeat to %s: %s", id, err.Error())
					} else {
						heartbeatResponse <- result
					}
				}()
			}
		case heartbeat := <-heartbeatResponse:
			if state := n.State(); state != Leader {
				n.logger.Printf("Warning: got AppendEntriesRPCResult in state %s", state)
				break
			}
			if heartbeat.Term > n.CurrentTerm() {
				n.setCurrentTerm(heartbeat.Term)
				n.becomeFollower()
			}
		case appendEntriesResult := <-appendEntriesResponse:
			if state := n.State(); state != Leader {
				n.logger.Printf("Warning: got AppendEntriesRPCResult in state %s", state)
				break
			}
			if appendEntriesResult.result.Term > n.CurrentTerm() {
				n.setCurrentTerm(appendEntriesResult.result.Term)
				n.becomeFollower()
				break
			}
			if appendEntriesResult.result.Success {
				n.logger.Printf("Agreed with %s", appendEntriesResult.id)
				n.nextIndex[appendEntriesResult.id] += len(appendEntriesResult.request.Entries)
				n.matchIndex[appendEntriesResult.id] = n.nextIndex[appendEntriesResult.id] - 1
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
				// matchCount := 1
				// commitIndex := n.StateMachine.CommitIndex()
				// for _, peerId := range n.otherNodeIds {
				// 	if n.matchIndex[peerId] >= commitIndex {
				// 		matchCount++
				// 	}
				// }
				// if matchCount*2 > len(n.nodes) {
				// 	commitIndex += len(appendEntriesResult.request.Entries)
				// 	if commitIndex > appendEntriesResult.request.LogIndex {
				// 		n.logger.Printf("set CommitIndex = %d", commitIndex)
				// 		n.StateMachine.SetCommitIndex(commitIndex)
				// 	}
				// 	n.StateMachine.Apply(n.StateMachine.CommitIndex())
				// 	appendEntriesResult.client <- n.StateMachine.CommitIndex()
				// }
			} else {
				n.mu.Lock()
				n.nextIndex[appendEntriesResult.id]--
				n.mu.Unlock()
				go func() {
					appendEntries := n.makeAppendEntries(appendEntriesResult.id)
					result, err := n.transport.IssueAppendEntries(context.Background(), appendEntries, appendEntriesResult.id)
					if err != nil {
						n.logger.Printf("could not reissue AppendEntries to %s: %s", appendEntriesResult.id, err.Error())
					} else {
						appendEntriesResponse <- appendEntriesFollowerResult{&appendEntries, result, appendEntriesResult.client, appendEntriesResult.id}
					}
				}()
			}
		case req := <-n.clientRequestCh:
			if state := n.State(); state != Leader {
				n.logger.Printf("Warning: got client request in state %s", state)
				req.commited <- 1
				break
			}
			n.logger.Printf("Client request: %s", req.cmd)
			n.StateMachine.AppendLogs(Entry{req.cmd, n.CurrentTerm()})
			for _, id := range n.otherNodeIds {
				go func() {
					appendEntries := n.makeAppendEntries(id)
					result, err := n.transport.IssueAppendEntries(context.Background(), appendEntries, id)
					if err != nil {
						n.logger.Printf("could not issue AppendEntries to %s: %s", id, err.Error())
					} else {
						appendEntriesResponse <- appendEntriesFollowerResult{&appendEntries, result, req.commited, id}
					}
				}()
			}
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
	for _, id := range n.otherNodeIds {
		n.nextIndex[id] = n.StateMachine.LastApplied() + 1
		n.matchIndex[id] = 0
	}
	n.electionTimer.Stop()
	n.heartbeatTimer.Reset(heartbeatPeriod)
}

func (n *Node) dlog(format string, v ...any) {
	if n.verbose {
		n.logger.Printf(format, v...)
	}
}
