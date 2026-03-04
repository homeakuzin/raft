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
	StateMachine         *StateMachine
	httpServer           *http.Server
	clientServer         *http.Server
	reportTick           *time.Ticker
	verbose              bool
	clientServerAddr     string
	stateUpdateRequestCh chan updateRequest
	requestVoteRpc       chan requestVoteRpcCall
	appendEntriesRpc     chan appendEntriesRpcCall
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
	}
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

func (n *Node) getVotesHave() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.VotesHave
}

func (n *Node) Shutdown() {
	n.shutdown <- struct{}{}
	if err := n.httpServer.Shutdown(context.Background()); err != nil {
		log.Printf("could not shutdown HTTP server: %s", err.Error())
	}
	if err := n.clientServer.Shutdown(context.Background()); err != nil {
		log.Printf("could not shutdown client HTTP server: %s", err.Error())
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

func (n *Node) eventLoop() {
	requestVoteResponse := make(chan RequestVoteResult)
	appendEntriesResponse := make(chan AppendEntriesResult)
	run := atomic.Bool{}
	run.Store(true)
	go func() {
		<-n.shutdown
		run.Store(false)
	}()

	for run.Load() {
		select {
		case appendEntries := <-n.appendEntriesRpc:
			n.dlog("AppendRequestRPC from %s", appendEntries.data.LeaderId)
			response := AppendEntriesResult{}
			if appendEntries.data.Term >= n.getCurrentTerm() {
				n.becomeFollower()
				n.setCurrentTerm(appendEntries.data.Term)
				response.Success = true
			}
			response.Term = n.getCurrentTerm()
			appendEntries.result <- response
		case requestVote := <-n.requestVoteRpc:
			n.dlog("RequestVoteRPC from %s", requestVote.data.CandidateId)
			response := RequestVoteResult{}
			if requestVote.data.Term > n.getCurrentTerm() || n.getVotedFor() == EmptyId {
				n.dlog("vote for %s", requestVote.data.CandidateId)
				n.becomeFollower()
				n.setCurrentTerm(requestVote.data.Term)
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
						n.dlog("RequestVoteResponse from %s: %+v", n.otherNodeIds[i], result)
						requestVoteResponse <- result
					}
				}()
			}
			if len(n.otherNodeIds) == 0 {
				n.becomeLeader()
			}
		case response := <-requestVoteResponse:
			if state := n.getState(); state != Candidate {
				log.Printf("Warning: got RequestVoteRPCResult in state %s", state)
				break
			}
			if response.Term > n.getCurrentTerm() {
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

		case response := <-appendEntriesResponse:
			if state := n.getState(); state != Leader {
				log.Printf("Warning: got AppendEntriesRPCResult in state %s", state)
				break
			}
			if response.Term > n.getCurrentTerm() {
				n.becomeFollower()
			}
		case <-n.heartbeatTimer.C:
			if state := n.getState(); state != Leader {
				log.Printf("Warning: got heartbeatTimer tick in state %s", state)
				break
			}
			appendEntriesCtx := context.Background()
			for i := range n.otherNodeIds {
				nodeHost := n.nodes[n.otherNodeIds[i]]
				go func() {
					n.dlog("issuing AppendEntries to %s", n.otherNodeIds[i])
					result, err := n.issueAppendEntries(appendEntriesCtx, AppendEntries{
						Term:     n.getCurrentTerm(),
						LeaderId: n.Id,
					}, nodeHost)
					if err != nil {
						n.dlog("could not issue AppendEntries to %s: %s", nodeHost, err.Error())
					} else {
						n.dlog("AppendEntriesResponse from %s: %+v", n.otherNodeIds[i], result)
						appendEntriesResponse <- result
					}
				}()
			}
		case req := <-n.stateUpdateRequestCh:
			log.Printf("Incoming request: %s", req.cmd)
			req.response <- 200
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

func (n *Node) becomeCandidate() {
	n.setState(Candidate)
	term := n.incrementTerm()
	n.setVotedFor(n.Id)
	n.setVotesHave(1)
	log.Printf("Term %d: election", term)
}

func (n *Node) becomeFollower() {
	n.setState(Follower)
	n.ElectionTimer.Reset(generateElectionTimeout())
	n.heartbeatTimer.Stop()
}

func (n *Node) becomeLeader() {
	log.Printf("Leader now")
	n.setState(Leader)
	n.ElectionTimer.Stop()
	n.heartbeatTimer.Reset(heartbeatPeriod)
}

func (n *Node) report() {
	n.mu.Lock()
	log.Printf("State => %s. Term => %d.", n.State, n.CurrentTerm)
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
		log.Printf("could not read body: %s", err.Error())
		w.WriteHeader(500)
		return
	}
	var requestVote RequestVote
	if err := json.Unmarshal(body, &requestVote); err != nil {
		log.Printf("invalid body: %s", err.Error())
		w.WriteHeader(400)
		return
	}
	responseCh := make(chan RequestVoteResult)
	n.requestVoteRpc <- requestVoteRpcCall{requestVote, responseCh}
	response := <-responseCh
	responseBody, err := json.Marshal(&response)
	if err != nil {
		log.Printf("could not serialize response body: %s", err.Error())
		w.WriteHeader(500)
		return
	}
	if _, err := w.Write(responseBody); err != nil {
		log.Printf("could not write response body: %s", err.Error())
		w.WriteHeader(500)
		return
	}
}

func (n *Node) handlerAppendEntries(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("could not read body: %s", err.Error())
		w.WriteHeader(500)
		return
	}
	var appendEntries AppendEntries
	if err := json.Unmarshal(body, &appendEntries); err != nil {
		log.Printf("invalid body: %s", err.Error())
		w.WriteHeader(400)
		return
	}
	responseCh := make(chan AppendEntriesResult)
	n.appendEntriesRpc <- appendEntriesRpcCall{appendEntries, responseCh}
	response := <-responseCh
	responseBody, err := json.Marshal(&response)
	if err != nil {
		log.Printf("could not serialize response body: %s", err.Error())
		w.WriteHeader(500)
		return
	}
	if _, err := w.Write(responseBody); err != nil {
		log.Printf("could not write response body: %s", err.Error())
		w.WriteHeader(500)
		return
	}
}

func (n *Node) runServer() error {
	handler := http.NewServeMux()
	handler.HandleFunc("POST /rpc/request-vote", n.handlerRequestVote)
	handler.HandleFunc("POST /rpc/append-entries", n.handlerAppendEntries)
	host := n.nodes[n.Id]
	log.Printf("Running HTTP server at %v", host)
	n.httpServer = &http.Server{Addr: host, Handler: handler}
	err := n.httpServer.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		err = nil
	}
	return nil
}

func (n *Node) dlog(format string, v ...any) {
	if n.verbose {
		log.Printf(format, v...)
	}
}

func main() {
	log.SetOutput(os.Stdout)
	flagNodes := flag.String("nodes", "", "id:host:port joined by semicolon. Example:\n\t0:0.0.0.1:1234;1:0.0.0.2:2345;2:0.0.0.3:3456")
	flagNodeId := flag.Int("id", int(EmptyId), "Current node ID")
	flagClientAddr := flag.String("clientaddr", "", "Address to serve HTTP clients")
	flag.Parse()

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

	nodeId := NodeId(*flagNodeId)
	if _, ok := nodes[NodeId(*flagNodeId)]; !ok {
		log.Fatalf("No node with id %d", *flagNodeId)
	}

	log.SetPrefix(fmt.Sprintf("[%s] ", nodeId))
	log.SetFlags(log.Lmicroseconds)
	node := NewNode(nodeId, nodes)
	if *flagVerbose {
		node.Verbose()
	}
	node.StartReporting()
	if *flagClientAddr != "" {
		go func() {
			if err := node.RunClientServer(*flagClientAddr); err != nil {
				log.Printf("Could not run client HTTP server: %s", err.Error())
			}
		}()
	}
	node.Run()
}
