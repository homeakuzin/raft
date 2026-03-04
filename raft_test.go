package main

import (
	"encoding/json"
	"hyperraft/pkg/asserts"
	"io"
	"net/http"
	"testing"
	"time"
)

func TestConsensys(t *testing.T) {
	nodes := map[NodeId]string{
		1: "localhost:5010",
		2: "localhost:5011",
		3: "localhost:5012",
	}
	n1 := NewNode(1, nodes) //.Verbose()
	n2 := NewNode(2, nodes) //.Verbose()
	n3 := NewNode(3, nodes) //.Verbose()
	nodeStructs := map[NodeId]*Node{
		1: n1,
		2: n2,
		3: n3,
	}
	go n1.Run()
	go n2.Run()
	go n3.Run()
	waitABit()
	n1State := nodeState(t, nodes[1])
	n2State := nodeState(t, nodes[2])
	n3State := nodeState(t, nodes[3])
	roles := map[State][]NodeId{
		Leader:    {},
		Follower:  {},
		Candidate: {},
	}
	roles[n1State.State] = append(roles[n1State.State], 1)
	roles[n2State.State] = append(roles[n2State.State], 2)
	roles[n3State.State] = append(roles[n3State.State], 3)
	asserts.Len(t, 2, roles[Follower])
	asserts.Len(t, 1, roles[Leader])
	asserts.Len(t, 0, roles[Candidate])
	firstTerm := n1State.Term
	asserts.True(t, firstTerm > 0)
	asserts.Equal(t, firstTerm, n2State.Term)
	asserts.Equal(t, firstTerm, n3State.Term)

	firstEverLeader := roles[Leader][0]
	t.Log("shutting down first leader")
	nodeStructs[firstEverLeader].Shutdown()
	t.Log("shut down first leader")
	waitABit()

	var newLeader *RaftState
	var follower *RaftState
	for _, id := range roles[Follower] {
		state := nodeState(t, nodes[id])
		if state.State == Leader {
			newLeader = &state
		} else if state.State == Follower {
			follower = &state
		} else if state.State == Candidate {
			t.Log("expected two nodes to be Leader and Follower, but have Candidate")
			t.Fail()
		}
	}
	asserts.NotEqual(t, nil, newLeader)
	asserts.NotEqual(t, nil, follower)
	secondTerm := newLeader.Term
	asserts.True(t, secondTerm > firstTerm)
	asserts.Equal(t, secondTerm, follower.Term)

	t.Logf("leader returns: %s", firstEverLeader)
	leaderIsBack := nodeStructs[firstEverLeader]
	go leaderIsBack.Run()
	waitABit()
	state := nodeState(t, nodes[firstEverLeader])
	asserts.Equal(t, Follower, state.State)
	asserts.Equal(t, secondTerm, state.Term)
}

func waitABit() {
	time.Sleep(time.Second)
}

func nodeState(t *testing.T, addr string) RaftState {
	resp, err := http.Get("http://" + addr + "/raft")
	if err != nil {
		t.Fatalf("/raft: %s", err.Error())
	}
	bodyjson, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %s", err.Error())
	}
	resp.Body.Close()
	var state RaftState
	if err := json.Unmarshal(bodyjson, &state); err != nil {
		t.Fatalf("unmarshal /raft: %s", err.Error())
	}
	return state
}
