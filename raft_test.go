package raft_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
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
		cluster := newCluster(t, ports.popPorts())
		defer cluster.stop(t.Context())

		commit1 := make(chan any, 1)
		commit2 := make(chan any, 1)
		commit3 := make(chan any, 1)
		commit1Cnt := atomic.Int32{}
		commit2Cnt := atomic.Int32{}
		commit3Cnt := atomic.Int32{}
		for _, node := range cluster.nodes {
			node.n.RegisterEventHandler(func(h *raft.EventHandler, event any) {
				switch data := event.(type) {
				case raft.EventCommit:
					if data.NewCommitIndex == 0 && commit1Cnt.Add(1) == int32(len(cluster.nodes)) {
						close(commit1)
					}
					if data.NewCommitIndex == 1 && commit2Cnt.Add(1) == int32(len(cluster.nodes)) {
						close(commit2)
					}
					if data.NewCommitIndex == 2 && commit3Cnt.Add(1) == int32(len(cluster.nodes)) {
						close(commit3)
					}
				}
			})
		}

		cluster.setup(t)
		initialLeader := cluster.leader(t)
		t.Log("sending two commands")
		cluster.command(t, []byte{1}, initialLeader)
		cluster.command(t, []byte{2}, initialLeader)
		cluster.wait1(commit2)
		cluster.assertHealthy(t)
		t.Log("sending last command")
		cluster.command(t, []byte{3}, initialLeader)
		cluster.wait1(commit3)
		cluster.assertHealthy(t)
	})
	t.Run("Cluster handles leader network partition", func(t *testing.T) {
		t.Parallel()
		cluster := newCluster(t, ports.popPorts())
		defer cluster.stop(t.Context())
		cluster.setup(t)

		initialLeader := cluster.leader(t)
		initialTerm := initialLeader.n.CurrentTerm()

		commit1 := make(chan any, 1)
		commit1Cnt := atomic.Int32{}
		for _, node := range cluster.nodes {
			node.n.RegisterEventHandler(func(h *raft.EventHandler, event any) {
				switch data := event.(type) {
				case raft.EventCommit:
					t.Log(node.n.Id, "commit", data.NewCommitIndex)
					if data.NewCommitIndex == 0 && commit1Cnt.Add(1) == int32(len(cluster.nodes)) {
						close(commit1)
					}
					h.Stop()
				}
			})
		}

		cluster.command(t, []byte{1}, initialLeader)
		cluster.wait1(commit1)

		cluster.assertHealthy(t)

		newLeaderCh := make(chan any)
		hs := make([]*raft.EventHandler, 0, 10)
		for _, n := range cluster.getNodes(raft.Follower) {
			hs = append(hs, n.n.RegisterEventHandler(func(h *raft.EventHandler, event any) {
				switch event.(type) {
				case raft.EventBecomeLeader:
					close(newLeaderCh)
				}
			}))
		}

		t.Logf("Partition leader %s", initialLeader.n.Id)
		for _, node := range cluster.nodes {
			node.network.unavailableNode.Store(int32(initialLeader.n.Id))
		}
		cluster.wait1(newLeaderCh)

		for _, h := range hs {
			h.Stop()
		}

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

		followerCommitCh := make(chan any)
		followers[0].n.RegisterEventHandler(func(h *raft.EventHandler, event any) {
			switch event.(type) {
			case raft.EventCommit:
				h.Stop()
				close(followerCommitCh)
			}
		})

		cluster.command(t, []byte{2}, newLeader)
		initialLeader.n.StateMachine.AppendLogs(raft.Entry{[]byte{3}, initialTerm})
		initialLeader.n.StateMachine.Apply(1)

		cluster.wait1(followerCommitCh)

		cluster.assertFollower(t, followers[0], newLeader)

		asserts.Len(t, 2, newLeader.n.StateMachine.Logs())
		asserts.Slice(t, []byte{2}, newLeader.n.StateMachine.Logs()[1].Command)
		asserts.Len(t, 2, initialLeader.n.StateMachine.Logs())
		asserts.Slice(t, []byte{3}, initialLeader.n.StateMachine.Logs()[1].Command)

		t.Logf("Restore %s availability", initialLeader.n.Id)
		initialLeaderCommitCh1 := make(chan any)
		initialLeader.n.RegisterEventHandler(func(h *raft.EventHandler, event any) {
			switch event.(type) {
			case raft.EventCommit:
				h.Stop()
				close(initialLeaderCommitCh1)
			}
		})
		for _, node := range cluster.nodes {
			node.network.unavailableNode.Store(-1)
		}
		cluster.wait1(initialLeaderCommitCh1)
		cluster.assertHealthy(t)
	})
	t.Run("Cluster handles leader high latency", func(t *testing.T) {
		t.Parallel()
		cluster := newCluster(t, ports.popPorts())
		defer cluster.stop(t.Context())
		cluster.setup(t)

		commit1 := make(chan any, 1)
		commit1Cnt := atomic.Int32{}
		for _, node := range cluster.nodes {
			node.n.RegisterEventHandler(func(h *raft.EventHandler, event any) {
				switch data := event.(type) {
				case raft.EventCommit:
					if data.NewCommitIndex == 0 && commit1Cnt.Add(1) == int32(len(cluster.nodes)) {
						close(commit1)
					}
				}
			})
		}

		initialLeader := cluster.leader(t)
		initialFollowers := cluster.getNodes(raft.Follower)
		cluster.command(t, []byte{1}, initialLeader)
		cluster.wait1(commit1)
		cluster.assertHealthy(t)

		t.Logf("Set latency to leader %s", initialLeader.n.Id)
		candidateCh := make(chan any, len(initialFollowers))
		leaderCh := make(chan any)
		followerCh := make(chan any)
		hs := make([]*raft.EventHandler, 0, 5)
		for _, n := range initialFollowers {
			hs = append(hs, n.n.RegisterEventHandler(func(h *raft.EventHandler, event any) {
				switch event.(type) {
				case raft.EventBecomeCandidate:
					candidateCh <- event
				case raft.EventBecomeLeader:
					close(leaderCh)
				case raft.EventTerm:
					close(followerCh)
				}
			}))
		}
		initialTerm := initialLeader.n.CurrentTerm()
		latency := time.Second
		initialLeader.network.latency.Store(int64(latency))
		cluster.wait1(candidateCh)
		cluster.wait1(leaderCh)
		cluster.wait1(followerCh)
		for _, h := range hs {
			h.Stop()
		}

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

		t.Log("sending command to the new leader")
		followerCh = make(chan any)
		followers[0].n.RegisterEventHandler(func(h *raft.EventHandler, event any) {
			switch event.(type) {
			case raft.EventCommit:
				h.Stop()
				close(followerCh)
			}
		})

		cluster.command(t, []byte{2}, newLeader)
		cluster.wait1(followerCh)
		cluster.assertFollower(t, followers[0], newLeader)

		t.Logf("Remove latency")
		initialLeaderUpdatedCh := make(chan any)
		initialLeader.n.RegisterEventHandler(func(h *raft.EventHandler, event any) {
			switch event.(type) {
			case raft.EventCommit:
				h.Stop()
				close(initialLeaderUpdatedCh)
			}
		})
		initialLeader.network.latency.Store(0)
		cluster.wait1(initialLeaderUpdatedCh)
		cluster.assertFollower(t, initialLeader, newLeader)
		cluster.assertHealthy(t)
	})
	t.Run("Cluster gives no shit when follower fails", func(t *testing.T) {
		t.Parallel()
		cluster := newCluster(t, ports.popPorts())
		defer cluster.stop(t.Context())
		cluster.setup(t)

		commit1 := make(chan any, 1)
		commit1Cnt := atomic.Int32{}
		for _, node := range cluster.nodes {
			node.n.RegisterEventHandler(func(h *raft.EventHandler, event any) {
				switch data := event.(type) {
				case raft.EventCommit:
					if data.NewCommitIndex == 0 && commit1Cnt.Add(1) == int32(len(cluster.nodes)) {
						close(commit1)
					}
					h.Stop()
				}
			})
		}

		cluster.command(t, []byte{1}, cluster.leader(t))
		cluster.wait1(commit1)
		cluster.assertHealthy(t)

		followers := cluster.getNodes(raft.Follower)
		t.Log("shutting down follower", followers[0].n.Id)
		failedFollower := followers[0]
		failedFollower.n.Shutdown(t.Context())

		leader := cluster.leader(t)

		leaderCommit := make(chan any)
		leader.n.RegisterEventHandler(func(h *raft.EventHandler, event any) {
			switch data := event.(type) {
			case raft.EventCommit:
				t.Log("leader.NewCommitIndex", data.NewCommitIndex)
				if data.NewCommitIndex == 2 {
					close(leaderCommit)
				}
				h.Stop()
			}
		})
		followerCommit := make(chan any)
		followers[1].n.RegisterEventHandler(func(h *raft.EventHandler, event any) {
			switch data := event.(type) {
			case raft.EventCommit:
				t.Log("follower.NewCommitIndex", data.NewCommitIndex)
				if data.NewCommitIndex == 2 {
					close(followerCommit)
				}
				h.Stop()
			}
		})

		t.Log("sending command to leader", leader.n.Id)
		cluster.command(t, []byte{2}, leader)
		t.Log("sending command to leader", leader.n.Id)
		cluster.command(t, []byte{3}, leader)
		cluster.wait1(leaderCommit)
		cluster.wait1(followerCommit)
		cluster.wait()
		cluster.assertFollower(t, followers[1], leader)

		go failedFollower.n.Run()
		cluster.wait()
		cluster.assertHealthy(t)
	})
	t.Run("Cluster recovers after leader failure", func(t *testing.T) {
		t.Parallel()
		cluster := newCluster(t, ports.popPorts())
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

func (p wrapProtocol) RequestVote(arg raft.RequestVote) (raft.RequestVoteResult, error) {
	time.Sleep(time.Duration(p.cond.latency.Load()))
	return p.actual.RequestVote(arg)
}

func (p wrapProtocol) AppendEntries(arg raft.AppendEntries) (raft.AppendEntriesResult, error) {
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
	t     testing.TB
	nodes map[raft.NodeId]node
}

func newCluster(t testing.TB, ports []int) *cluster {
	cluster := &cluster{t, make(map[raft.NodeId]node)}
	peers := make(map[raft.NodeId]string)
	for i, port := range ports {
		peers[raft.NodeId(i)] = fmt.Sprintf("127.0.0.1:%d", port)
	}
	for i := range ports {
		id := raft.NodeId(i)
		storage := &storage.ListStorage{}
		logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
		transport := &transport{actual: raft.HTTPTransport(id, peers, logger), cond: networkConditions{}}
		transport.cond.unavailableNode.Store(-1)
		n := raft.NewNode(id, peers, transport, storage, logger)
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
	leaderLogs := leader.n.StateMachine.Logs()
	asserts.Len(t, leaderCommit+1, leaderLogs)

	followers := c.getNodes(raft.Follower)
	asserts.Len(t, 2, followers)
	for _, node := range followers {
		c.assertFollower(t, node, leader)
	}
	t.Log("cluster is healthy")
}

func (c *cluster) assertFollower(t *testing.T, node, leader node) {
	leaderTerm := leader.n.CurrentTerm()
	asserts.Equal(t, leaderTerm, node.n.CurrentTerm())

	leaderCommit := leader.n.StateMachine.CommitIndex()
	nodeCommit := node.n.StateMachine.CommitIndex()
	asserts.EqualEx(t, leaderCommit, nodeCommit, "node commit index %d is not equal to leader %d", nodeCommit, leaderCommit)

	logs := node.n.StateMachine.Logs()
	leaderLogs := leader.n.StateMachine.Logs()
	asserts.EqualEx(t, leaderCommit+1, len(logs), "leader %s commit is %d, but %s has %d logs", leader.n.Id, leaderCommit+1, node.n.Id, len(logs))
	for i := range logs {
		asserts.SliceEx(t, leaderLogs[i].Command, logs[i].Command, "folower %s logs (%v) differ from leader %s (%v)", node.n.Id, logs, leader.n.Id, leaderLogs)
	}
}

func (c *cluster) command(t *testing.T, cmd []byte, node node) {
	logsBefore := node.n.StateMachine.Logs()
	node.n.ClientCommand(t.Context(), cmd)
	logs := node.n.StateMachine.Logs()
	asserts.Equal(t, len(logsBefore)+1, len(logs))
	asserts.Slice(t, cmd, logs[len(logs)-1].Command)
}

func (c *cluster) leader(t *testing.T) node {
	t.Helper()
	leaders := c.getNodes(raft.Leader)
	asserts.Len(t, 1, leaders)
	return leaders[0]
}

func (c *cluster) setup(t *testing.T) {
	t.Helper()
	t.Log("starting cluster")
	c.run()
	followers := atomic.Int32{}
	followersCh := make(chan any)
	leaderCh := make(chan any, 5)
	hs := make([]*raft.EventHandler, 0, 10)
	for _, node := range c.nodes {
		hs = append(hs, node.n.RegisterEventHandler(func(h *raft.EventHandler, event any) {
			switch event.(type) {
			case raft.EventBecomeFollower:
				if followers.Add(1) == 2 {
					close(followersCh)
				}
			}
		}))
		hs = append(hs, node.n.RegisterEventHandler(func(h *raft.EventHandler, event any) {
			switch event.(type) {
			case raft.EventBecomeLeader:
				close(leaderCh)
			}
		}))
	}
	c.wait1(leaderCh)
	c.wait1(followersCh)
	c.assertHealthy(t)
	for _, h := range hs {
		h.Stop()
	}
	t.Log("cluster is running")
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

func (s *cluster) wait1(ch1 <-chan any) {
	s.t.Helper()
	timer := time.NewTimer(time.Millisecond * 1000)
	defer timer.Stop()
	select {
	case <-timer.C:
		s.t.Fatal("event timed out")
	case <-ch1:
	}
}

func (s *cluster) wait2(ch1, ch2 <-chan any) {
	timer := time.NewTimer(time.Millisecond * 500)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ch1:
	case <-ch2:
	}
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
