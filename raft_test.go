package raft

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"raft/pkg/asserts"
	"testing"
	"time"
)

// TODO it sometimes blocks indefinitely
func TestConsensys(t *testing.T) {
	nodes := map[NodeId]string{
		1: "localhost:5010",
		2: "localhost:5011",
		3: "localhost:5012",
	}
	nodeClientAddrs := map[NodeId]string{
		1: "localhost:5510",
		2: "localhost:5511",
		3: "localhost:5512",
	}
	kv1 := &KVStorage{state: map[string]string{}}
	kv2 := &KVStorage{state: map[string]string{}}
	kv3 := &KVStorage{state: map[string]string{}}
	kvs := map[NodeId]*KVStorage{
		1: kv1,
		2: kv2,
		3: kv3,
	}
	n1 := NewNode(1, nodes, kv1).LogPrefixId() //.Verbose()
	n2 := NewNode(2, nodes, kv2).LogPrefixId() //.Verbose()
	n3 := NewNode(3, nodes, kv3).LogPrefixId() //.Verbose()
	nodeStructs := map[NodeId]*Node{
		1: n1,
		2: n2,
		3: n3,
	}
	go n1.Run()
	go n2.Run()
	go n3.Run()
	go RunClientServer(nodeClientAddrs[1], n1, kv1)
	go RunClientServer(nodeClientAddrs[2], n2, kv1)
	go RunClientServer(nodeClientAddrs[3], n3, kv1)
	waitABit()
	n1State := nodeState(t, nodeClientAddrs[1])
	n2State := nodeState(t, nodeClientAddrs[2])
	n3State := nodeState(t, nodeClientAddrs[3])
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
	nodeStructs[firstEverLeader].Shutdown()
	waitABit()

	var newLeader *RaftState
	var follower *RaftState
	for _, id := range roles[Follower] {
		state := nodeState(t, nodeClientAddrs[id])
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

	leaderIsBack := nodeStructs[firstEverLeader]
	go leaderIsBack.Run()
	go RunClientServer(nodeClientAddrs[firstEverLeader], nodeStructs[firstEverLeader], kvs[firstEverLeader])
	waitABit()
	state := nodeState(t, nodeClientAddrs[firstEverLeader])
	asserts.Equal(t, Follower, state.State)
	asserts.Equal(t, secondTerm, state.Term)
}

func TestKeyValueReplication(t *testing.T) {
	nodes := map[NodeId]string{
		1: "localhost:5020",
		2: "localhost:5021",
		3: "localhost:5022",
	}
	nodeClientAddrs := map[NodeId]string{
		1: "localhost:5530",
		2: "localhost:5531",
		3: "localhost:5532",
	}
	kv1 := &KVStorage{state: map[string]string{}}
	kv2 := &KVStorage{state: map[string]string{}}
	kv3 := &KVStorage{state: map[string]string{}}
	kvs := map[NodeId]*KVStorage{
		1: kv1,
		2: kv2,
		3: kv3,
	}
	n1 := NewNode(1, nodes, kv1).LogPrefixId() //.Verbose()
	n2 := NewNode(2, nodes, kv2).LogPrefixId() //.Verbose()
	n3 := NewNode(3, nodes, kv3).LogPrefixId() //.Verbose()
	go n1.Run()
	go n2.Run()
	go n3.Run()
	go RunClientServer(nodeClientAddrs[1], n1, kv1)
	go RunClientServer(nodeClientAddrs[2], n2, kv2)
	go RunClientServer(nodeClientAddrs[3], n3, kv3)
	waitABit()
	n1State := nodeState(t, nodeClientAddrs[1])
	n2State := nodeState(t, nodeClientAddrs[2])
	n3State := nodeState(t, nodeClientAddrs[3])
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

	leader := roles[Leader][0]
	timeout, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	sendSetRequest(t, timeout, nodeClientAddrs[leader], "x", []byte{'3'})
	waitABit()
	for _, addr := range nodeClientAddrs {
		value, status := sendGetRequest(t, addr, "x")
		asserts.EqualEx(fmt.Sprintf("expected status 200 for node %s", addr), t, 200, status)
		asserts.EqualEx(fmt.Sprintf("expected x=3 for node %s", addr), t, "3", string(value))
	}

	nodeStructs := map[NodeId]*Node{
		1: n1,
		2: n2,
		3: n3,
	}
	nodeStructs[leader].Shutdown()
	waitABit()
	for _, id := range roles[Follower] {
		addr := nodeClientAddrs[id]
		value, status := sendGetRequest(t, addr, "x")
		asserts.EqualEx(fmt.Sprintf("expected status 200 for node %s", addr), t, 200, status)
		asserts.EqualEx(fmt.Sprintf("expected x=3 for node %s", addr), t, "3", string(value))
	}

	leaderIsBack := nodeStructs[leader]
	go leaderIsBack.Run()
	go RunClientServer(nodeClientAddrs[leader], leaderIsBack, kvs[leader])
	waitABit()
	value, status := sendGetRequest(t, nodeClientAddrs[leader], "x")
	asserts.EqualEx(fmt.Sprintf("expected status 200 for node %s", nodeClientAddrs[leader]), t, 200, status)
	asserts.EqualEx(fmt.Sprintf("expected x=3 for node %s", nodeClientAddrs[leader]), t, "3", string(value))
}

func sendSetRequest(t *testing.T, ctx context.Context, addr string, key string, value []byte) {
	req, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("http://%s/%s", addr, key), bytes.NewBuffer(value))
	if err != nil {
		return
	}
	client := http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("sendSetRequest: %s", err.Error())
	}
	resp.Body.Close()
	asserts.Equal(t, 200, resp.StatusCode)
}

func sendGetRequest(t *testing.T, addr string, key string) ([]byte, int) {
	resp, err := http.Get("http://" + addr + "/" + key)
	if err != nil {
		t.Fatalf("sendGetRequest: %s", err.Error())
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	asserts.ErrNil(t, err)
	return body, resp.StatusCode
}

// TODO listen to some kind of event maybe?
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
	asserts.Equal(t, 200, resp.StatusCode)
	var state RaftState
	if err := json.Unmarshal(bodyjson, &state); err != nil {
		t.Fatalf("unmarshal /raft: %s", err.Error())
	}
	return state
}
