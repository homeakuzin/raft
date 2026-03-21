package raft_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	. "github.com/homeakuzin/raft"
	"github.com/homeakuzin/raft/pkg/asserts"
	"github.com/homeakuzin/raft/storage"
)

type node struct {
	n       *Node
	storage *storage.ListStorage
}

type cluster struct {
	nodes map[NodeId]node
}

func newCluster(ports []int) *cluster {
	cluster := &cluster{}
	cluster.nodes = make(map[NodeId]node)
	peers := make(map[NodeId]string)
	for i, port := range ports {
		peers[NodeId(i)] = fmt.Sprintf("127.0.0.1:%d", port)
	}
	for i := range ports {
		id := NodeId(i)
		storage := &storage.ListStorage{}
		n := NewNode(id, peers, HTTPTransport(id, peers), storage).LogPrefixId()
		cluster.nodes[id] = node{n, storage}
	}
	return cluster
}

func (c *cluster) getNodes(state State) []node {
	ns := make([]node, 0, len(c.nodes))
	for _, n := range c.nodes {
		if n.n.State() == state {
			ns = append(ns, n)
		}
	}
	return ns
}

func (c *cluster) run() {
	for _, node := range c.nodes {
		go node.n.Run()
	}
}

func (c *cluster) stop(ctx context.Context) {
	for _, node := range c.nodes {
		node.n.Shutdown(ctx)
	}
}

func (s *cluster) wait() {
	time.Sleep(time.Millisecond * 500)
}

type portsStack struct {
	sync.Mutex
	ports []int
}

func (s *portsStack) popPorts() []int {
	return s.popPortsN(3)
}

func (s *portsStack) popPortsN(n int) []int {
	s.Lock()
	defer s.Unlock()
	if n > len(s.ports) {
		panic(fmt.Sprintf("popPorts: tried to pop %d ports but only have %d", n, len(s.ports)))
	}
	popped := s.ports[:n]
	s.ports = s.ports[n:]
	return popped
}

func TestRaft(t *testing.T) {
	ports := portsStack{}
	startPort := 30000
	nPorts := 3000
	ports.ports = make([]int, 0, nPorts)
	for p := startPort; p < startPort+nPorts; p++ {
		ports.ports = append(ports.ports, p)
	}

	t.Run("Cluster recovers after leader failure", func(t *testing.T) {
		t.Parallel()
		cluster := newCluster(ports.popPorts())
		defer cluster.stop(t.Context())
		cluster.run()
		cluster.wait()
		leaders := cluster.getNodes(Leader)
		followers := cluster.getNodes(Follower)
		asserts.Len(t, 1, leaders)
		asserts.Len(t, 2, followers)

		initialLeader := leaders[0]
		initialTerm := initialLeader.n.CurrentTerm()
		asserts.Equal(t, initialTerm, followers[0].n.CurrentTerm())
		asserts.Equal(t, initialTerm, followers[1].n.CurrentTerm())

		firstCommand := []byte{1}
		initialLeader.n.ClientCommand(t.Context(), firstCommand)
		commitIndex := 0
		asserts.Equal(t, commitIndex, initialLeader.n.StateMachine.CommitIndex())
		cmds := initialLeader.storage.Commands()
		asserts.Len(t, 1, cmds)
		asserts.Slice(t, firstCommand, cmds[0])

		cluster.wait()
		for i := 0; i < 2; i++ {
			asserts.Equal(t, commitIndex, initialLeader.n.StateMachine.CommitIndex())
			cmds := initialLeader.storage.Commands()
			asserts.Len(t, 1, cmds)
			asserts.Slice(t, firstCommand, cmds[0])
		}

		initialLeader.n.Shutdown(t.Context())
		cluster.wait()

		leaders = cluster.getNodes(Leader)
		followers = cluster.getNodes(Follower)
		asserts.Len(t, 1, leaders)
		asserts.Len(t, 1, followers)

		newTerm := leaders[0].n.CurrentTerm()
		asserts.Gt(t, initialTerm, newTerm)
		asserts.Equal(t, newTerm, followers[0].n.CurrentTerm())

		secondCommand := []byte{2}
		leaders[0].n.ClientCommand(t.Context(), secondCommand)
		secondCommitIndex := 1
		asserts.Equal(t, secondCommitIndex, leaders[0].n.StateMachine.CommitIndex())
		cmds = leaders[0].storage.Commands()
		asserts.Len(t, 2, cmds)
		asserts.Slice(t, secondCommand, cmds[1])
		cluster.wait()
		asserts.Equal(t, secondCommitIndex, followers[0].n.StateMachine.CommitIndex())
		cmds = followers[0].storage.Commands()
		asserts.Len(t, 2, cmds)
		asserts.Slice(t, secondCommand, cmds[1])

		dead := cluster.getNodes(Dead)
		asserts.Len(t, 1, dead)
		asserts.Equal(t, initialLeader, dead[0])

		go initialLeader.n.Run()
		cluster.wait()
		initialLeaderNewTerm := initialLeader.n.CurrentTerm()
		asserts.Equal(t, newTerm, initialLeaderNewTerm)
		asserts.Equal(t, secondCommitIndex, initialLeader.n.StateMachine.CommitIndex())
		cmds = initialLeader.storage.Commands()
		asserts.Len(t, 2, cmds)
		asserts.Slice(t, secondCommand, cmds[1])
	})
}
