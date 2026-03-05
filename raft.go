package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var heartbeatPeriod = 50 * time.Millisecond
var electionTimeout = 150 * time.Millisecond
var electionTimeoutDelta = 150 * time.Millisecond

var flagVerbose = flag.Bool("v", false, "enables verbose output")

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

type updateRequest struct {
	cmd      Command
	response chan int
}

type Node struct {
	mu                   sync.Mutex
	Id                   NodeId
	VotedFor             NodeId
	VotesHave            int
	CurrentTerm          int
	State                State
	ElectionTimer        *time.Timer
	heartbeatTimer       *time.Timer
	shutdown             chan struct{}
	nodes                map[NodeId]string
	otherNodeIds         []NodeId
	nextIndex            map[NodeId]int
	matchIndex           map[NodeId]int
	StateMachine         *StateMachine
	httpServer           *http.Server
	clientServer         *http.Server
	reportTick           *time.Ticker
	verbose              bool
	clientServerAddr     string
	requestVoteRpc       chan requestVoteRpcCall
	appendEntriesRpc     chan appendEntriesRpcCall
	commitCh             chan int
	stateUpdateRequestCh chan updateRequest
	logger               *log.Logger
}

func (n *Node) Verbose() *Node {
	n.verbose = true
	return n
}

func (n *Node) getCurrentTerm() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.CurrentTerm
}

func NewNode(id NodeId, nodes map[NodeId]string) *Node {
	otherNodeIds := []NodeId{}
	for otherId := range nodes {
		if id != otherId {
			otherNodeIds = append(otherNodeIds, otherId)
		}
	}
	return &Node{Id: id,
		nodes:                nodes,
		StateMachine:         NewStateMachine(),
		stateUpdateRequestCh: make(chan updateRequest),
		shutdown:             make(chan struct{}),
		otherNodeIds:         otherNodeIds,
		requestVoteRpc:       make(chan requestVoteRpcCall),
		appendEntriesRpc:     make(chan appendEntriesRpcCall),
		nextIndex:            make(map[NodeId]int),
		matchIndex:           make(map[NodeId]int),
		logger:               log.Default(),
	}
}

func (n *Node) LogPrefixId() *Node {
	n.logger.SetPrefix(fmt.Sprintf("[%s] ", n.Id))
	return n
}

func (n *Node) incrementTerm() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.CurrentTerm++
	return n.CurrentTerm
}

func (n *Node) setCurrentTerm(term int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.CurrentTerm = term
}

func (n *Node) getState() State {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.State
}

func (n *Node) setState(s State) State {
	n.mu.Lock()
	defer n.mu.Unlock()
	was := n.State
	n.State = s
	return was
}

func (n *Node) getVotedFor() NodeId {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.VotedFor
}

func (n *Node) setVotedFor(id NodeId) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.VotedFor = id
}

func (n *Node) incrementVotesHave() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.VotesHave++
	return n.VotesHave
}

func (n *Node) setVotesHave(value int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.VotesHave = value
}

func (n *Node) Shutdown() {
	n.shutdown <- struct{}{}
	if err := n.httpServer.Shutdown(context.Background()); err != nil {
		n.logger.Printf("could not shutdown HTTP server: %s", err.Error())
	}
	if err := n.clientServer.Shutdown(context.Background()); err != nil {
		n.logger.Printf("could not shutdown client HTTP server: %s", err.Error())
	}
	if n.reportTick != nil {
		n.reportTick.Stop()
	}
}

func (n *Node) StartReporting() {
	n.reportTick = time.NewTicker(time.Second)
	go func() {
		for range n.reportTick.C {
			n.report()
		}
	}()
}

func (n *Node) Run() {
	go func() {
		if err := n.runServer(); err != nil {
			n.Shutdown()
		}
	}()
	n.VotedFor = EmptyId
	n.ElectionTimer = time.NewTimer(generateElectionTimeout())

	n.heartbeatTimer = time.NewTimer(heartbeatPeriod)
	n.heartbeatTimer.Stop()
	n.eventLoop()
}

type appendEntriesFollowerResult struct {
	request *AppendEntries
	result  AppendEntriesResult
	client  chan int
	id      NodeId
}

func (n *Node) eventLoop() {
	requestVoteResponse := make(chan RequestVoteResult)
	heartbeatResponse := make(chan AppendEntriesResult)
	appendEntriesResponse := make(chan appendEntriesFollowerResult)
	run := atomic.Bool{}
	run.Store(true)
	go func() {
		<-n.shutdown
		run.Store(false)
	}()

	for run.Load() {
		select {
		case appendEntries := <-n.appendEntriesRpc:
			response := AppendEntriesResult{}
			if appendEntries.data.Term >= n.getCurrentTerm() {
				n.setCurrentTerm(appendEntries.data.Term)
				n.becomeFollower()
				response.Success = true
			}
			if appendEntries.data.LeaderCommit > n.StateMachine.CommitIndex() {
				n.StateMachine.Apply(appendEntries.data.LeaderCommit)
				n.StateMachine.SetCommitIndex(appendEntries.data.LeaderCommit)
				n.logger.Printf("Applied commit index %d", appendEntries.data.LeaderCommit)
			}
			if len(appendEntries.data.Entries) > 0 {
				if appendEntries.data.PrevLogIndex > 0 {
					lastEntry, ok := n.StateMachine.At(appendEntries.data.PrevLogIndex)
					if ok {
						if lastEntry.Term == appendEntries.data.PrevLogTerm {
							response.Success = true
							n.StateMachine.AppendLogs(appendEntries.data.Entries...)
							n.logger.Printf("Replicated %d entries on a Follower.", len(appendEntries.data.Entries))
						} else {
							n.StateMachine.DeleteFrom(appendEntries.data.PrevLogIndex)
						}
					}
				} else {
					response.Success = true
					n.StateMachine.AppendLogs(appendEntries.data.Entries...)
					n.logger.Printf("Replicated %d entries on a Follower.", len(appendEntries.data.Entries))
				}
			}
			response.Term = n.getCurrentTerm()
			appendEntries.result <- response
		case requestVote := <-n.requestVoteRpc:
			n.dlog("RequestVoteRPC from %s", requestVote.data.CandidateId)
			response := RequestVoteResult{}
			if requestVote.data.Term > n.getCurrentTerm() || n.getVotedFor() == EmptyId {
				n.setCurrentTerm(requestVote.data.Term)
				n.becomeFollower()
				n.setVotedFor(requestVote.data.CandidateId)
				response.VoteGranted = true
			}
			response.Term = n.getCurrentTerm()
			requestVote.result <- response
		case <-n.ElectionTimer.C:
			n.becomeCandidate()
			for i := range n.otherNodeIds {
				nodeHost := n.nodes[n.otherNodeIds[i]]
				go func() {
					n.dlog("issuing RequestVote to %s", n.otherNodeIds[i])
					result, err := n.issueRequestVote(context.Background(), nodeHost)
					if err != nil {
						n.dlog("could not issue RequestVote to %s: %s", nodeHost, err.Error())
					} else {
						requestVoteResponse <- result
					}
				}()
			}
		case response := <-requestVoteResponse:
			if state := n.getState(); state != Candidate {
				n.logger.Printf("Warning: got RequestVoteRPCResult in state %s", state)
				break
			}
			if response.Term > n.getCurrentTerm() {
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
			if state := n.getState(); state != Leader {
				n.logger.Printf("Warning: got heartbeatTimer tick in state %s", state)
				break
			}
			appendEntriesCtx := context.Background()
			for _, id := range n.otherNodeIds {
				nodeHost := n.nodes[id]
				go func() {
					result, err := n.issueAppendEntries(appendEntriesCtx, n.makeAppendEntries(id), nodeHost)
					if err != nil {
						n.dlog("could not issue heartbeat to %s: %s", nodeHost, err.Error())
					} else {
						heartbeatResponse <- result
					}
				}()
			}
		case heartbeat := <-heartbeatResponse:
			if state := n.getState(); state != Leader {
				n.logger.Printf("Warning: got AppendEntriesRPCResult in state %s", state)
				break
			}
			if heartbeat.Term > n.getCurrentTerm() {
				n.setCurrentTerm(heartbeat.Term)
				n.becomeFollower()
			}
		case appendEntriesResult := <-appendEntriesResponse:
			if state := n.getState(); state != Leader {
				n.logger.Printf("Warning: got AppendEntriesRPCResult in state %s", state)
				break
			}
			if appendEntriesResult.result.Term > n.getCurrentTerm() {
				n.setCurrentTerm(appendEntriesResult.result.Term)
				n.becomeFollower()
				break
			}
			if appendEntriesResult.result.Success {
				n.logger.Printf("Agreed with %s", appendEntriesResult.id)
				n.nextIndex[appendEntriesResult.id] += len(appendEntriesResult.request.Entries)
				n.matchIndex[appendEntriesResult.id] = n.nextIndex[appendEntriesResult.id] - 1
				matchCount := 1
				commitIndex := n.StateMachine.CommitIndex()
				for _, peerId := range n.otherNodeIds {
					if n.matchIndex[peerId] > commitIndex {
						matchCount++
					}
				}
				if matchCount*2 > len(n.otherNodeIds)+1 {
					commitIndex += len(appendEntriesResult.request.Entries)
					n.logger.Printf("set CommitIndex = %d", commitIndex)
					n.StateMachine.SetCommitIndex(commitIndex)
					n.StateMachine.Apply(commitIndex)
					appendEntriesResult.client <- 200
				}
			} else {
				n.mu.Lock()
				n.nextIndex[appendEntriesResult.id]--
				n.mu.Unlock()
				go func() {
					nodeHost := n.nodes[appendEntriesResult.id]
					appendEntries := n.makeAppendEntries(appendEntriesResult.id)
					result, err := n.issueAppendEntries(context.Background(), appendEntries, nodeHost)
					if err != nil {
						n.logger.Printf("could not issue AppendEntries to %s: %s", nodeHost, err.Error())
					} else {
						appendEntriesResponse <- appendEntriesFollowerResult{&appendEntries, result, appendEntriesResult.client, appendEntriesResult.id}
					}
				}()
			}
		case req := <-n.stateUpdateRequestCh:
			n.logger.Printf("Incoming request: %s", req.cmd)
			n.StateMachine.AppendLogs(Entry{req.cmd, n.getCurrentTerm()})
			for _, id := range n.otherNodeIds {
				nodeHost := n.nodes[id]
				go func() {
					appendEntries := n.makeAppendEntries(id)
					result, err := n.issueAppendEntries(context.Background(), appendEntries, nodeHost)
					if err != nil {
						n.logger.Printf("could not issue AppendEntries to %s: %s", nodeHost, err.Error())
					} else {
						appendEntriesResponse <- appendEntriesFollowerResult{&appendEntries, result, req.response, id}
					}
				}()
			}
		}

		switch n.getState() {
		case Follower:
			n.ElectionTimer.Reset(generateElectionTimeout())
		case Candidate:
			n.ElectionTimer.Reset(generateElectionTimeout())
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
		Term:         n.getCurrentTerm(),
		LeaderId:     n.Id,
		LeaderCommit: n.StateMachine.CommitIndex(),
		Entries:      entries,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
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
	n.ElectionTimer.Reset(generateElectionTimeout())
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
	n.ElectionTimer.Stop()
	n.heartbeatTimer.Reset(heartbeatPeriod)
}

func (n *Node) report() {
	n.mu.Lock()
	n.logger.Printf("State => %s. Term => %d.", n.State, n.CurrentTerm)
	n.mu.Unlock()
}

func (n *Node) issueRequestVote(ctx context.Context, host string) (result RequestVoteResult, err error) {
	requestVote := RequestVote{
		Term:        n.getCurrentTerm(),
		CandidateId: n.Id,
	}
	body, err := json.Marshal(&requestVote)
	if err != nil {
		return
	}

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

func (n *Node) issueAppendEntries(ctx context.Context, appendEntries AppendEntries, host string) (AppendEntriesResult, error) {
	var result AppendEntriesResult
	body, err := json.Marshal(&appendEntries)
	if err != nil {
		return result, err
	}

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

func (n *Node) handlerRequestVote(w http.ResponseWriter, r *http.Request) {
	time.Sleep(50 * time.Millisecond)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		n.logger.Printf("could not read body: %s", err.Error())
		w.WriteHeader(500)
		return
	}
	var requestVote RequestVote
	if err := json.Unmarshal(body, &requestVote); err != nil {
		n.logger.Printf("invalid body: %s", err.Error())
		w.WriteHeader(400)
		return
	}
	responseCh := make(chan RequestVoteResult)
	n.requestVoteRpc <- requestVoteRpcCall{requestVote, responseCh}
	response := <-responseCh
	responseBody, err := json.Marshal(&response)
	if err != nil {
		n.logger.Printf("could not serialize response body: %s", err.Error())
		w.WriteHeader(500)
		return
	}
	if _, err := w.Write(responseBody); err != nil {
		n.logger.Printf("could not write response body: %s", err.Error())
		w.WriteHeader(500)
		return
	}
}

func (n *Node) handlerAppendEntries(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		n.logger.Printf("could not read body: %s", err.Error())
		w.WriteHeader(500)
		return
	}
	var appendEntries AppendEntries
	if err := json.Unmarshal(body, &appendEntries); err != nil {
		n.logger.Printf("invalid body: %s", err.Error())
		w.WriteHeader(400)
		return
	}
	responseCh := make(chan AppendEntriesResult)
	n.appendEntriesRpc <- appendEntriesRpcCall{appendEntries, responseCh}
	response := <-responseCh
	responseBody, err := json.Marshal(&response)
	if err != nil {
		n.logger.Printf("could not serialize response body: %s", err.Error())
		w.WriteHeader(500)
		return
	}
	if _, err := w.Write(responseBody); err != nil {
		n.logger.Printf("could not write response body: %s", err.Error())
		w.WriteHeader(500)
		return
	}
}

func (n *Node) runServer() error {
	handler := http.NewServeMux()
	handler.HandleFunc("POST /rpc/request-vote", n.handlerRequestVote)
	handler.HandleFunc("POST /rpc/append-entries", n.handlerAppendEntries)
	host := n.nodes[n.Id]
	n.logger.Printf("Running HTTP server at %v", host)
	n.httpServer = &http.Server{Addr: host, Handler: handler}
	err := n.httpServer.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		err = nil
	}
	return nil
}

func (n *Node) dlog(format string, v ...any) {
	if n.verbose {
		n.logger.Printf(format, v...)
	}
}

func main() {
	log.SetFlags(log.Lmicroseconds)
	log.SetOutput(os.Stdout)

	flagNodes := flag.String("nodes", "", "id:host:port joined by semicolon. Example:\n\t0:0.0.0.1:1234;1:0.0.0.2:2345;2:0.0.0.3:3456")
	flagNodeId := flag.Int("id", int(EmptyId), "Current node ID")
	flagClientAddr := flag.String("clientaddr", "", "Address to serve HTTP clients")
	flagMetricsAddr := flag.String("metricsaddr", "", "Address to serve prometheus metrics")
	flagReport := flag.Bool("report", false, "Whether to report node state every second")
	flag.Parse()

	nodeId := NodeId(*flagNodeId)
	log.SetPrefix(fmt.Sprintf("[%s] ", nodeId))

	nodes := make(map[NodeId]string)
	nodeParts := strings.Split(*flagNodes, ";")
	for i := range nodeParts {
		idHostAndPort := strings.Split(nodeParts[i], ":")
		if len(idHostAndPort) != 3 {
			flag.Usage()
			log.Fatal("Incorrect -nodes usage")
		}
		nodeIdInt, err := strconv.Atoi(idHostAndPort[0])
		if err != nil {
			log.Fatal("Incorrect -nodes usage")
		}
		nodes[NodeId(nodeIdInt)] = idHostAndPort[1] + ":" + idHostAndPort[2]
	}

	if len(nodes) < 3 {
		log.Fatalf("We work with 3 nodes minimum")
	}

	if _, ok := nodes[NodeId(*flagNodeId)]; !ok {
		log.Fatalf("No node with id %d", *flagNodeId)
	}

	if *flagMetricsAddr != "" {
		reg := prometheus.NewRegistry()
		reg.MustRegister(
			collectors.NewGoCollector(),
			collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		)
		http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
		log.Printf("exposing prometheus metrics at %s", *flagMetricsAddr)
		go http.ListenAndServe(*flagMetricsAddr, nil)
	}

	node := NewNode(nodeId, nodes)
	if *flagVerbose {
		node.Verbose()
	}
	if *flagReport {
		node.StartReporting()
	}
	if *flagClientAddr != "" {
		go func() {
			if err := node.RunClientServer(*flagClientAddr); err != nil {
				log.Printf("Could not run client HTTP server: %s", err.Error())
			}
		}()
	}
	node.Run()
}
