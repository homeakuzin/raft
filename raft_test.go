package raft_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
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

	t.Run("Replication works", func(t *testing.T) {
		t.Parallel()
		cluster := newCluster(ports.popPorts())
		defer cluster.stop(t.Context())
		cluster.setup(t)
		initialLeader := cluster.leader(t)
		cluster.command(t, []byte{1}, initialLeader)
		cluster.command(t, []byte{2}, initialLeader)
		cluster.wait()
		cluster.assertHealthy(t)
		cluster.command(t, []byte{3}, initialLeader)
		cluster.wait()
		cluster.assertHealthy(t)
	})
	t.Run("Cluster handles leader network partition", func(t *testing.T) {
		t.Parallel()
		cluster := newCluster(ports.popPorts())
		defer cluster.stop(t.Context())
		cluster.setup(t)

		initialLeader := cluster.leader(t)
		initialTerm := initialLeader.n.CurrentTerm()
		cluster.command(t, []byte{1}, initialLeader)
		cluster.wait()
		cluster.assertHealthy(t)

		t.Logf("Partition leader %s", initialLeader.n.Id)
		for _, node := range cluster.nodes {
			node.network.unavailableNode.Store(int32(initialLeader.n.Id))
		}
		cluster.wait()
		leaders := cluster.getNodes(raft.Leader)
		followers := cluster.getNodes(raft.Follower)
		asserts.Len(t, 2, leaders)
		asserts.Len(t, 1, followers)
		var newLeader node
		if leaders[0].n.Id == initialLeader.n.Id {
			newLeader = leaders[1]
		} else {
			newLeader = leaders[0]
		}
		asserts.Gt(t, initialTerm, newLeader.n.CurrentTerm())
		cluster.assertFollower(t, followers[0], newLeader)

		cluster.command(t, []byte{2}, newLeader)
		initialLeader.n.StateMachine.AppendLogs(raft.Entry{[]byte{3}, initialTerm})
		initialLeader.n.StateMachine.Apply(1)
		cluster.wait()
		cluster.assertFollower(t, followers[0], newLeader)
		asserts.Len(t, 2, newLeader.storage.Commands())
		asserts.Slice(t, []byte{2}, newLeader.storage.Commands()[1])
		asserts.Len(t, 2, initialLeader.storage.Commands())
		asserts.Slice(t, []byte{3}, initialLeader.storage.Commands()[1])

		t.Logf("Restore %s availability", initialLeader.n.Id)
		for _, node := range cluster.nodes {
			node.network.unavailableNode.Store(-1)
		}
		initialLeader.n.Debug()
		cluster.wait()
		cluster.assertHealthy(t)
	})
	t.Run("Cluster handles leader high latency", func(t *testing.T) {
		t.Parallel()
		cluster := newCluster(ports.popPorts())
		defer cluster.stop(t.Context())
		cluster.setup(t)

		initialLeader := cluster.leader(t)
		cluster.command(t, []byte{1}, initialLeader)
		cluster.wait()
		cluster.assertHealthy(t)

		t.Logf("Set latency to leader %s", initialLeader.n.Id)
		latency := time.Second
		initialLeader.network.latency.Store(int64(latency))
		initialTerm := initialLeader.n.CurrentTerm()
		cluster.waitFor(time.Millisecond * 500)

		leaders := cluster.getNodes(raft.Leader)
		followers := cluster.getNodes(raft.Follower)
		asserts.Len(t, 2, leaders)
		asserts.Len(t, 1, followers)
		var newLeader node
		if leaders[0].n.Id == initialLeader.n.Id {
			newLeader = leaders[1]
		} else {
			newLeader = leaders[0]
		}
		asserts.Gt(t, initialTerm, newLeader.n.CurrentTerm())
		cluster.waitFor(time.Millisecond * 500)
		cluster.assertFollower(t, followers[0], newLeader)

		cluster.command(t, []byte{2}, newLeader)
		cluster.wait()
		cluster.assertFollower(t, followers[0], newLeader)

		t.Logf("Remove latency")
		initialLeader.network.latency.Store(0)
		cluster.waitFor(latency)
		cluster.assertFollower(t, initialLeader, newLeader)
		cluster.assertHealthy(t)
	})
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
	network *networkConditions
}

type networkConditions struct {
	unavailableNode atomic.Int32
	latency         atomic.Int64
}

type wrapProtocol struct {
	node   node
	actual raft.RaftProtocol
	cond   *networkConditions
}

func (p wrapProtocol) RequestVote(arg raft.RequestVote) raft.RequestVoteResult {
	time.Sleep(time.Duration(p.cond.latency.Load()))
	return p.actual.RequestVote(arg)
}

func (p wrapProtocol) AppendEntries(arg raft.AppendEntries) raft.AppendEntriesResult {
	time.Sleep(time.Duration(p.cond.latency.Load()))
	return p.actual.AppendEntries(arg)
}

type transport struct {
	node   node
	actual raft.Transport
	cond   networkConditions
}

func (t *transport) IssueRequestVote(ctx context.Context, data raft.RequestVote, node raft.NodeId) (raft.RequestVoteResult, error) {
	unavailableNodeId := raft.NodeId(t.cond.unavailableNode.Load())
	if unavailableNodeId == node || t.node.n.Id == unavailableNodeId {
		return raft.RequestVoteResult{}, errors.New("node is unavailable")
	}
	time.Sleep(time.Duration(t.cond.latency.Load()))
	return t.actual.IssueRequestVote(ctx, data, node)
}

func (t *transport) IssueAppendEntries(ctx context.Context, data raft.AppendEntries, node raft.NodeId) (raft.AppendEntriesResult, error) {
	unavailableNodeId := raft.NodeId(t.cond.unavailableNode.Load())
	if unavailableNodeId == node || t.node.n.Id == unavailableNodeId {
		return raft.AppendEntriesResult{}, errors.New("node is unavailable")
	}
	time.Sleep(time.Duration(t.cond.latency.Load()))
	return t.actual.IssueAppendEntries(ctx, data, node)
}

func (t *transport) Serve(protocol raft.RaftProtocol) error {
	return t.actual.Serve(wrapProtocol{actual: protocol, cond: &t.cond})
}

func (t *transport) Shutdown(ctx context.Context) error {
	return t.actual.Shutdown(ctx)
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
		transport := &transport{actual: raft.HTTPTransport(id, peers), cond: networkConditions{}}
		transport.cond.unavailableNode.Store(-1)
		n := raft.NewNode(id, peers, transport, storage).LogPrefixId()
		node := node{n, storage, &transport.cond}
		transport.node = node
		cluster.nodes[id] = node
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
	asserts.EqualEx(t, leaderCommit+1, len(logs), fmt.Sprintf("leader %s commit is %d, but %s has %d logs", leader.n.Id, leaderCommit+1, node.n.Id, len(logs)))
	for i := range logs {
		asserts.SliceEx(t, leaderLogs[i], logs[i], fmt.Sprintf("folower %s logs (%v) differ from leader %s (%v)", node.n.Id, logs, leader.n.Id, leaderLogs))
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
	t.Helper()
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

func (s *cluster) waitFor(d time.Duration) {
	time.Sleep(d)
}

func (s *cluster) wait() {
	s.waitFor(time.Millisecond * 500)
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
