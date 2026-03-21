package raft_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/homeakuzin/raft"
	"github.com/homeakuzin/raft/pkg/asserts"
	"github.com/homeakuzin/raft/storage"
)

func TestRaft(t *testing.T) {
	ports := portsStack{}
	startPort := 30000
	nPorts := 3000
	ports.ports = make([]int, 0, nPorts)
	for p := startPort; p < startPort+nPorts; p++ {
		ports.ports = append(ports.ports, p)
	}

	t.Run("Cluster gives no shit when follower fails", func(t *testing.T) {
		t.Parallel()
		cluster := newCluster(ports.popPorts())
		defer cluster.stop(t.Context())
		cluster.setup(t)

		cluster.command(t, []byte{1}, cluster.leader(t))
		cluster.wait()
		cluster.assertHealthy(t)

		followers := cluster.getNodes(raft.Follower)
		failedFollower := followers[0]
		failedFollower.n.Shutdown(t.Context())

		leader := cluster.leader(t)
		cluster.command(t, []byte{2}, leader)
		cluster.command(t, []byte{3}, leader)
		cluster.wait()
		cluster.assertFollower(t, followers[1], leader)

		go failedFollower.n.Run()
		cluster.wait()
		cluster.assertHealthy(t)
	})
	t.Run("Cluster recovers after leader failure", func(t *testing.T) {
		t.Parallel()
		cluster := newCluster(ports.popPorts())
		defer cluster.stop(t.Context())
		cluster.setup(t)

		initialLeader := cluster.leader(t)
		cluster.command(t, []byte{1}, initialLeader)
		cluster.wait()
		cluster.assertHealthy(t)

		initialTerm := initialLeader.n.CurrentTerm()
		initialLeader.n.Shutdown(t.Context())
		cluster.wait()

		leaders := cluster.getNodes(raft.Leader)
		followers := cluster.getNodes(raft.Follower)
		asserts.Len(t, 1, leaders)
		asserts.Len(t, 1, followers)

		newTerm := leaders[0].n.CurrentTerm()
		asserts.Gt(t, initialTerm, newTerm)
		cluster.assertFollower(t, followers[0], leaders[0])

		cluster.command(t, []byte{2}, leaders[0])

		cluster.wait()
		cluster.assertFollower(t, followers[0], leaders[0])

		dead := cluster.getNodes(raft.Dead)
		asserts.Len(t, 1, dead)
		asserts.Equal(t, initialLeader, dead[0])

		go initialLeader.n.Run()
		cluster.wait()

		cluster.assertHealthy(t)
	})
}

type node struct {
	n       *raft.Node
	storage *storage.ListStorage
}

type cluster struct {
	nodes map[raft.NodeId]node
}

func newCluster(ports []int) *cluster {
	cluster := &cluster{}
	cluster.nodes = make(map[raft.NodeId]node)
	peers := make(map[raft.NodeId]string)
	for i, port := range ports {
		peers[raft.NodeId(i)] = fmt.Sprintf("127.0.0.1:%d", port)
	}
	for i := range ports {
		id := raft.NodeId(i)
		storage := &storage.ListStorage{}
		n := raft.NewNode(id, peers, raft.HTTPTransport(id, peers), storage).LogPrefixId()
		cluster.nodes[id] = node{n, storage}
	}
	return cluster
}

func (c *cluster) getNodes(state raft.State) []node {
	ns := make([]node, 0, len(c.nodes))
	for _, n := range c.nodes {
		if n.n.State() == state {
			ns = append(ns, n)
		}
	}
	return ns
}

func (c *cluster) assertHealthy(t *testing.T) {
	leader := c.leader(t)
	leaderCommit := leader.n.StateMachine.CommitIndex()
	leaderLogs := leader.storage.Commands()
	asserts.Len(t, leaderCommit+1, leaderLogs)

	followers := c.getNodes(raft.Follower)
	asserts.Len(t, 2, followers)
	for _, node := range followers {
		c.assertFollower(t, node, leader)
	}
}

func (c *cluster) assertFollower(t *testing.T, node, leader node) {
	leaderTerm := leader.n.CurrentTerm()
	asserts.Equal(t, leaderTerm, node.n.CurrentTerm())

	leaderCommit := leader.n.StateMachine.CommitIndex()
	asserts.Equal(t, leaderCommit, node.n.StateMachine.CommitIndex())

	logs := node.storage.Commands()
	leaderLogs := leader.storage.Commands()
	asserts.Len(t, leaderCommit+1, logs)
	for i := range logs {
		asserts.Slice(t, leaderLogs[i], logs[i])
	}
}

func (c *cluster) command(t *testing.T, cmd []byte, node node) {
	logsBefore := node.storage.Commands()
	node.n.ClientCommand(t.Context(), cmd)
	logs := node.storage.Commands()
	asserts.Equal(t, len(logsBefore)+1, len(logs))
	asserts.Slice(t, cmd, logs[len(logs)-1])
}

func (c *cluster) leader(t *testing.T) node {
	leaders := c.getNodes(raft.Leader)
	asserts.Len(t, 1, leaders)
	return leaders[0]
}

func (c *cluster) setup(t *testing.T) {
	c.run()
	c.wait()
	c.assertHealthy(t)
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
