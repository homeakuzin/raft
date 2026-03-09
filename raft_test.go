package raft_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	. "github.com/homeakuzin/raft"
	"github.com/homeakuzin/raft/pkg/asserts"
	"github.com/homeakuzin/raft/storage"
)

// TODO it sometimes blocks indefinitely
func TestConsensys(t *testing.T) {
	nodes := map[NodeId]string{
		1: "localhost:5010",
		2: "localhost:5011",
		3: "localhost:5012",
	}

	kv1 := storage.NewKVStorage()
	kv2 := storage.NewKVStorage()
	kv3 := storage.NewKVStorage()

	n1 := NewNode(1, nodes, HTTPTransport(1, nodes), kv1).LogPrefixId() //.Verbose()
	n2 := NewNode(2, nodes, HTTPTransport(2, nodes), kv2).LogPrefixId() //.Verbose()
	n3 := NewNode(3, nodes, HTTPTransport(3, nodes), kv3).LogPrefixId() //.Verbose()
	nodeStructs := map[NodeId]*Node{
		1: n1,
		2: n2,
		3: n3,
	}
	go n1.Run()
	go n2.Run()
	go n3.Run()
	waitABit()
	roles := map[State][]NodeId{
		Leader:    {},
		Follower:  {},
		Candidate: {},
	}
	roles[n1.State()] = append(roles[n1.State()], 1)
	roles[n2.State()] = append(roles[n2.State()], 2)
	roles[n3.State()] = append(roles[n3.State()], 3)
	asserts.Len(t, 2, roles[Follower])
	asserts.Len(t, 1, roles[Leader])
	asserts.Len(t, 0, roles[Candidate])
	firstTerm := n1.CurrentTerm()
	asserts.True(t, firstTerm > 0)
	asserts.Equal(t, firstTerm, n2.CurrentTerm())
	asserts.Equal(t, firstTerm, n3.CurrentTerm())

	firstEverLeader := roles[Leader][0]
	nodeStructs[firstEverLeader].Shutdown(t.Context())
	waitABit()

	var newLeaderTerm int
	var followerTerm int
	for _, id := range roles[Follower] {
		node := nodeStructs[id]
		if node.State() == Leader {
			newLeaderTerm = node.CurrentTerm()
		} else if node.State() == Follower {
			followerTerm = node.CurrentTerm()
		} else if node.State() == Candidate {
			t.Log("expected two nodes to be Leader and Follower, but have Candidate")
			t.Fail()
		}
	}
	asserts.NotEqual(t, 0, newLeaderTerm)
	asserts.NotEqual(t, 0, followerTerm)
	secondTerm := newLeaderTerm
	asserts.True(t, secondTerm > firstTerm)
	asserts.Equal(t, secondTerm, followerTerm)

	leaderIsBack := nodeStructs[firstEverLeader]
	go leaderIsBack.Run()
	waitABit()
	asserts.Equal(t, Follower, nodeStructs[firstEverLeader].State())
	asserts.Equal(t, secondTerm, nodeStructs[firstEverLeader].CurrentTerm())
}

func TestKeyValueReplication(t *testing.T) {
	nodes := map[NodeId]string{
		1: "localhost:5020",
		2: "localhost:5021",
		3: "localhost:5022",
	}

	kv1 := storage.NewKVStorage()
	kv2 := storage.NewKVStorage()
	kv3 := storage.NewKVStorage()
	kvs := map[NodeId]*storage.KVStorage{
		1: kv1,
		2: kv2,
		3: kv3,
	}
	n1 := NewNode(1, nodes, HTTPTransport(1, nodes), kv1).LogPrefixId() //.Verbose()
	n2 := NewNode(2, nodes, HTTPTransport(2, nodes), kv2).LogPrefixId() //.Verbose()
	n3 := NewNode(3, nodes, HTTPTransport(3, nodes), kv3).LogPrefixId() //.Verbose()
	nodeStructs := map[NodeId]*Node{
		1: n1,
		2: n2,
		3: n3,
	}
	go n1.Run()
	go n2.Run()
	go n3.Run()
	waitABit()
	roles := map[State][]NodeId{
		Leader:    {},
		Follower:  {},
		Candidate: {},
	}
	roles[n1.State()] = append(roles[n1.State()], 1)
	roles[n2.State()] = append(roles[n2.State()], 2)
	roles[n3.State()] = append(roles[n3.State()], 3)
	asserts.Len(t, 2, roles[Follower])
	asserts.Len(t, 1, roles[Leader])
	asserts.Len(t, 0, roles[Candidate])
	t.Log("cluster is set up")

	leader := roles[Leader][0]
	timeout, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	nodeStructs[leader].ClientCommand(timeout, []byte(`{"Action":0,"Key":"x","Value":"3"}`))

	waitABit()
	for id := range nodes {
		value, ok := kvs[id].Get("x")
		asserts.EqualEx(fmt.Sprintf("%s expected to have x=3", id), t, "3", string(value))
		asserts.True(t, ok)
	}

	nodeStructs[leader].Shutdown(t.Context())
	waitABit()
	for _, id := range roles[Follower] {
		value, ok := kvs[id].Get("x")
		asserts.EqualEx(fmt.Sprintf("%s expected to have x=3", id), t, "3", string(value))
		asserts.True(t, ok)
	}

	leaderIsBack := nodeStructs[leader]
	go leaderIsBack.Run()
	waitABit()

	value, ok := kvs[leader].Get("x")
	asserts.True(t, ok)
	asserts.Equal(t, "3", string(value))
}

// TODO listen to some kind of event maybe?
func waitABit() {
	time.Sleep(time.Second)
}
