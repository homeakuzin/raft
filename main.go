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
	"strconv"
	"sync"
	"time"
)

var electionTimeout = 1 * time.Second
var electionTimeoutDelta = 250 * time.Millisecond

func generateElectionTimeout() time.Duration {
	return electionTimeout
	delta := time.Duration(rand.Int63n(int64(electionTimeoutDelta)*2) - int64(electionTimeoutDelta))
	return electionTimeout + delta
}

var nodes = map[NodeId]string{
	0: "localhost:4000",
	1: "localhost:4001",
	2: "localhost:4002",
}

type Node struct {
	Id          NodeId
	VotedFor    NodeId
	VotedForMu  sync.Mutex
	CurrentTerm int
	State       State
}

func (n *Node) Run() error {
	host := nodes[n.Id]
	var otherNodeIds []NodeId
	for id := range nodes {
		if n.Id != id {
			otherNodeIds = append(otherNodeIds, id)
		}
	}
	log.Printf("spinning a node at %v", host)

	go n.runServer(host)

	electionTimer := time.NewTimer(generateElectionTimeout())

	for {
		if n.State == Leader {
			ctx := context.Background()
			wg := sync.WaitGroup{}
			for i := range otherNodeIds {
				nodeHost := nodes[otherNodeIds[i]]
				wg.Add(1)
				go func() {
					defer wg.Done()
					log.Printf("issuing AppendEntries to node-%d", otherNodeIds[i])
					result, err := n.issueAppendEntries(ctx, nodeHost)
					if err != nil {
						log.Printf("could not issue AppendEntries to %s: %s", nodeHost, err.Error())
					}
					_ = result
				}()
			}
		} else {
			<-electionTimer.C

			log.Printf("starting an election")
			n.VotedFor = EmptyId
			n.CurrentTerm++
			n.State = Candidate

			wg := sync.WaitGroup{}
			ctx := context.Background()

			votes := make(chan RequestVoteResult, len(otherNodeIds))

			for i := range otherNodeIds {
				nodeHost := nodes[otherNodeIds[i]]
				wg.Add(1)
				go func() {
					defer wg.Done()
					log.Printf("issuing RequestVote to node-%d", otherNodeIds[i])
					result, err := n.issueRequestVote(ctx, nodeHost)
					if err != nil {
						log.Printf("could not issue RequestVote to %s: %s", nodeHost, err.Error())
					} else {
						votes <- result
					}
				}()
			}

			wg.Wait()
			close(votes)

			votesNumCh := make(chan int)
			go func() {
				votesNum := 0
				for result := range votes {
					if result.VoteGranted {
						votesNum++
					}
				}
				votesNumCh <- votesNum
			}()

			votesNum := <-votesNumCh
			log.Printf("I got %d votes", votesNum)
			if votesNum > len(nodes)/2 {
				log.Printf("I am a leader now")
				n.State = Leader
			}
			electionTimer.Reset(generateElectionTimeout())
		}
	}
}

func (n *Node) issueRequestVote(ctx context.Context, host string) (result RequestVoteResult, err error) {
	requestVote := RequestVote{
		Term:        n.CurrentTerm,
		CandidateId: n.Id,
	}
	body, err := json.Marshal(&requestVote)
	if err != nil {
		return
	}

	req, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("http://%s/request-vote", host), bytes.NewBuffer(body))
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

func (n *Node) issueAppendEntries(ctx context.Context, host string) (result AppendEntriesResult, err error) {
	appendEntries := AppendEntries{
		Term:     n.CurrentTerm,
		LeaderId: n.Id,
	}
	body, err := json.Marshal(&appendEntries)
	if err != nil {
		return
	}

	req, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("http://%s/append-entries", host), bytes.NewBuffer(body))
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

func (n *Node) requestVoteHandler(w http.ResponseWriter, r *http.Request) {
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
	response := RequestVoteResult{Term: n.CurrentTerm}
	if requestVote.Term >= n.CurrentTerm {
		n.VotedForMu.Lock()
		if n.VotedFor == EmptyId {
			log.Printf("I give my vote to %v", requestVote.CandidateId)
			n.VotedFor = requestVote.CandidateId
			response.VoteGranted = true
		}
		n.VotedForMu.Unlock()
	}
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
	http.HandleFunc("POST /request-vote", n.requestVoteHandler)
	if err := http.ListenAndServe(host, nil); err != nil {
		log.Fatal(err.Error())
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
	log.SetPrefix(fmt.Sprintf("node-%d. ", nodeId))
	node := Node{Id: nodeId}
	if err := node.Run(); err != nil {
		log.Fatal(err.Error())
	}
}
