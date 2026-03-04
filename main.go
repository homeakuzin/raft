package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"sync"
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

var nodes = map[NodeId]string{
	0: "localhost:4000",
	1: "localhost:4001",
	2: "localhost:4002",
}

type Node struct {
	mu            sync.Mutex
	Id            NodeId
	VotedFor      NodeId
	VotesHave     int
	CurrentTerm   int
	State         State
	ElectionTimer *time.Timer
	OtherNodeIds  []NodeId
	StateMachine  *StateMachine
}

func (n *Node) getCurrentTerm() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.CurrentTerm
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

func (n *Node) setState(s State) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.State = s
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

func (n *Node) Run() error {
	host := nodes[n.Id]
	for id := range nodes {
		if n.Id != id {
			n.OtherNodeIds = append(n.OtherNodeIds, id)
		}
	}
	log.Printf("spinning a node at %v", host)

	go n.runServer(host)

	n.VotedFor = EmptyId
	n.ElectionTimer = time.NewTimer(generateElectionTimeout())
	defer n.ElectionTimer.Stop()

	heartbeatTimer := time.NewTimer(heartbeatPeriod)
	defer heartbeatTimer.Stop()

	reportTick := time.NewTicker(time.Second)
	defer reportTick.Stop()
	go func() {
		for range reportTick.C {
			n.report()
		}
	}()

	requestVoteResponse := make(chan RequestVoteResult)
	appendEntriesResponse := make(chan AppendEntriesResult)

	for {
		state := n.getState()
		if state == Follower {
			select {
			case <-n.ElectionTimer.C:
				n.ElectionTimer.Reset(generateElectionTimeout())
				n.setState(Candidate)
				term := n.incrementTerm()
				n.setVotedFor(n.Id)
				n.setVotesHave(1)
				log.Printf("term %d: election", term)
				for i := range n.OtherNodeIds {
					nodeHost := nodes[n.OtherNodeIds[i]]
					go func() {
						dlog("issuing RequestVote to %s", n.OtherNodeIds[i])
						result, err := n.issueRequestVote(context.Background(), nodeHost)
						if err != nil {
							dlog("could not issue RequestVote to %s: %s", nodeHost, err.Error())
						} else {
							dlog("RequestVoteResponse from %s: %+v", n.OtherNodeIds[i], result)
							requestVoteResponse <- result
						}
					}()
				}
			}
		} else if state == Candidate {
			select {
			case <-n.ElectionTimer.C:
				n.ElectionTimer.Reset(generateElectionTimeout())
				term := n.incrementTerm()
				n.setVotedFor(n.Id)
				n.setVotesHave(1)
				dlog("term %d: election", term)
				for i := range n.OtherNodeIds {
					nodeHost := nodes[n.OtherNodeIds[i]]
					go func() {
						dlog("issuing RequestVote to %s", n.OtherNodeIds[i])
						result, err := n.issueRequestVote(context.Background(), nodeHost)
						if err != nil {
							dlog("could not issue RequestVote to %s: %s", nodeHost, err.Error())
						} else {
							dlog("RequestVoteResponse from %s: %+v", n.OtherNodeIds[i], result)
							requestVoteResponse <- result
						}
					}()
				}
			case response := <-requestVoteResponse:
				dlog("RequestVoteResponse from someone: %+v", response)
				n.ElectionTimer.Reset(generateElectionTimeout())
				if response.Term > n.getCurrentTerm() {
					n.setState(Follower)
					break
				}
				if response.VoteGranted {
					// check vote sources?
					votes := n.incrementVotesHave()
					if votes > len(nodes)/2 {
						dlog("Leader now")
						n.setState(Leader)
					}
				}
			}
		} else if state == Leader {
			select {
			case <-heartbeatTimer.C:
				appendEntriesCtx := context.Background()
				wg := &sync.WaitGroup{}
				for i := range n.OtherNodeIds {
					nodeHost := nodes[n.OtherNodeIds[i]]
					wg.Add(1)
					go func() {
						defer wg.Done()
						dlog("issuing AppendEntries to %s", n.OtherNodeIds[i])
						result, err := n.issueAppendEntries(appendEntriesCtx, nodeHost)
						if err != nil {
							dlog("could not issue AppendEntries to %s: %s", nodeHost, err.Error())
						} else {
							dlog("AppendEntriesResponse from %s: %+v", n.OtherNodeIds[i], result)
							appendEntriesResponse <- result
						}
					}()
				}
				heartbeatTimer.Reset(heartbeatPeriod)
			case response := <-appendEntriesResponse:
				if response.Term > n.getCurrentTerm() {
					n.ElectionTimer.Reset(generateElectionTimeout())
					n.setState(Follower)
					break
				}
			}
		}
	}
}

func (n *Node) report() {
	n.mu.Lock()
	log.Printf("State => %s. Term => %d. VotedFor => %s.", n.State, n.CurrentTerm, n.VotedFor)
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

func (n *Node) issueAppendEntries(ctx context.Context, host string) (AppendEntriesResult, error) {
	var result AppendEntriesResult
	appendEntries := AppendEntries{
		Term:     n.getCurrentTerm(),
		LeaderId: n.Id,
	}
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
	dlog("RequestVoteRPC from %s", requestVote.CandidateId)
	response := RequestVoteResult{}
	if requestVote.Term > n.getCurrentTerm() || n.getVotedFor() == EmptyId {
		dlog("vote for %s", requestVote.CandidateId)
		n.ElectionTimer.Reset(generateElectionTimeout())
		n.setState(Follower)
		n.setCurrentTerm(requestVote.Term)
		n.setVotedFor(requestVote.CandidateId)
		response.VoteGranted = true
	}
	response.Term = n.getCurrentTerm()
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
	dlog("AppendRequestRPC from %s", appendEntries.LeaderId)
	response := AppendEntriesResult{}
	if appendEntries.Term >= n.getCurrentTerm() {
		n.ElectionTimer.Reset(generateElectionTimeout())
		n.setCurrentTerm(appendEntries.Term)
		n.setState(Follower)
		response.Success = true
	}
	response.Term = n.getCurrentTerm()
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

func (n *Node) runServer(host string) {
	log.SetOutput(os.Stdout)
	// http.HandleFunc("POST /{key}", n.handlerRequestVote)
	// http.HandleFunc("DELETE /{key}", n.handlerRequestVote)
	http.HandleFunc("POST /rpc/request-vote", n.handlerRequestVote)
	http.HandleFunc("POST /rpc/append-entries", n.handlerAppendEntries)
	if err := http.ListenAndServe(host, nil); err != nil {
		log.Fatal(err.Error())
	}
}

func dlog(format string, v ...any) {
	if *flagVerbose {
		log.Printf(format, v...)
	}
}

func main() {
	flag.Parse()
	idstr := flag.Arg(0)
	if idstr == "" {
		log.Fatal("Provide node ID")
	}
	nodeIdInt, err := strconv.Atoi(idstr)
	if _, ok := nodes[NodeId(nodeIdInt)]; err != nil || !ok {
		log.Fatal("Incorrect ID provided")
	}

	nodeId := NodeId(nodeIdInt)
	log.SetPrefix(fmt.Sprintf("[%s] ", nodeId))
	log.SetFlags(log.Lmicroseconds)
	node := Node{Id: nodeId, StateMachine: NewStateMachine()}
	if err := node.Run(); err != nil {
		log.Fatal(err.Error())
	}
}
