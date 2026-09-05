package main_test

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"sync"
	"testing"
	"time"

	. "github.com/homeakuzin/raft"
	main "github.com/homeakuzin/raft"
	"github.com/stretchr/testify/require"
)

var flagDebugLevel = flag.Int("debug", 0, "node debug level")
var flagPprofAddr = flag.String("pprof", "", "serve pprof on the given address, e.g. localhost:6060")

var testingTimeouts = NodeTimeouts{
	Election:  25 * time.Millisecond,
	Heartbeat: 5 * time.Millisecond,
}

const pollInterval = time.Millisecond * 20

var nodeLogColors = map[NodeId]string{
	Node1: "\x1b[34m",
	Node2: "\x1b[32m",
	Node3: "\x1b[35m",
}

func TestMain(m *testing.M) {
	flag.Parse()
	if *flagPprofAddr != "" {
		ln, err := net.Listen("tcp", *flagPprofAddr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to listen for pprof on %s: %v\n", *flagPprofAddr, err)
			os.Exit(1)
		}
		go func() {
			if err := http.Serve(ln, nil); err != nil && err != http.ErrServerClosed {
				fmt.Fprintf(os.Stderr, "pprof server failed: %v\n", err)
			}
		}()
	}
	os.Exit(m.Run())
}

func TestLeaderIsElected(t *testing.T) {
	t.Parallel()
	cluster := newHTTPNetworkTestCluster(t)
	cluster.Run(t.Context())
	snapshot := cluster.waitHealthy()
	for _, followerID := range snapshot.followerIDs {
		require.Equal(t, snapshot.leaderTerm, snapshot.terms[followerID])
	}
}

func TestLeaderReplicatesClientCommands(t *testing.T) {
	t.Parallel()
	cluster := newHTTPNetworkTestCluster(t)
	cluster.Run(t.Context())
	cluster.waitHealthy()
	leader := cluster.leader()
	t.Logf("send first command to %s", leader.Id())
	cmd1 := []byte{'r', 'a', 'f', 't'}
	require.NoError(t, leader.ClientCommand(t.Context(), cmd1))
	cluster.waitNodesHaveCommitIndex(1, cluster.nodes)
	for _, n := range cluster.nodes {
		logs := n.StateMachine().Logs()
		require.Len(t, logs, 1, n.Id())
		require.Equal(t, cmd1, logs[0].Data, n.Id())
	}

	t.Logf("send second command to %s", leader.Id())
	cmd2 := []byte{'g', 'o'}
	require.NoError(t, leader.ClientCommand(t.Context(), cmd2))
	cluster.waitNodesHaveCommitIndex(2, cluster.nodes)
	for _, n := range cluster.nodes {
		logs := n.StateMachine().Logs()
		require.Len(t, logs, 2, n.Id())
		require.Equal(t, cmd1, logs[0].Data, n.Id())
		require.Equal(t, cmd2, logs[1].Data, n.Id())
	}
}

func TestNodeRecoversStateAfterFailure(t *testing.T) {
	t.Parallel()
	cluster := newHTTPNetworkTestCluster(t)
	cluster.Run(t.Context())
	clusterSnapshot := cluster.waitHealthy()

	initialLeader := cluster.leader()
	t.Logf("send first command to %s", initialLeader.Id())
	cmd1 := []byte{'r', 'a', 'f', 't'}
	require.NoError(t, initialLeader.ClientCommand(t.Context(), cmd1))
	cluster.waitNodesHaveCommitIndex(1, cluster.nodes)

	t.Logf("cut leader %s", initialLeader.Id())
	cluster.conditions.Cut(initialLeader.Id(), clusterSnapshot.followerIDs[0])
	cluster.conditions.Cut(initialLeader.Id(), clusterSnapshot.followerIDs[1])
	clusterSnapshot = waitLeaderAmong(t, cluster.nodesByID(clusterSnapshot.followerIDs...))

	t.Logf("send second command to %s", clusterSnapshot.leaderID)
	cmd2 := []byte{'g', 'o'}
	require.NoError(t, cluster.nodesByID(clusterSnapshot.leaderID)[0].ClientCommand(t.Context(), cmd2))
	cluster.waitNodesHaveCommitIndex(2, cluster.nodesByID(clusterSnapshot.followerIDs...))

	t.Log("restore initial leader")
	cluster.conditions.Heal(initialLeader.Id(), clusterSnapshot.followerIDs[0])
	cluster.conditions.Heal(initialLeader.Id(), clusterSnapshot.leaderID)
	cluster.waitNodesHaveCommitIndex(2, cluster.nodes)
	for _, n := range cluster.nodes {
		logs := n.StateMachine().Logs()
		require.Len(t, logs, 2)
		require.Equal(t, cmd2, logs[1].Data)
	}
}

func TestNewLeaderIsElectedWhenInitialIsUnavailable(t *testing.T) {
	t.Parallel()

	cluster := newHTTPNetworkTestCluster(t)
	cluster.Run(t.Context())

	initial := cluster.waitHealthy()
	cluster.conditions.Cut(initial.leaderID, initial.followerIDs[0])
	cluster.conditions.Cut(initial.leaderID, initial.followerIDs[1])

	majority := waitLeaderAmong(t, cluster.nodesByID(initial.followerIDs...))
	require.Greater(t, majority.leaderTerm, initial.leaderTerm)

	cluster.conditions.Heal(initial.leaderID, initial.followerIDs[0])
	cluster.conditions.Heal(initial.leaderID, initial.followerIDs[1])

	healed := cluster.waitHealthy()

	require.Greater(t, healed.leaderTerm, initial.leaderTerm)
	require.GreaterOrEqual(t, healed.leaderTerm, majority.leaderTerm)
	for _, term := range healed.terms {
		require.Equal(t, healed.leaderTerm, term)
	}
}

func TestFollowerPartitionDoesNotBreakMajorityLeader(t *testing.T) {
	t.Parallel()

	cluster := newHTTPNetworkTestCluster(t)
	cluster.Run(t.Context())

	initial := cluster.waitHealthy()
	partitionedFollower := initial.followerIDs[0]
	majorityFollower := initial.followerIDs[1]

	cluster.conditions.Cut(partitionedFollower, initial.leaderID)
	cluster.conditions.Cut(partitionedFollower, majorityFollower)

	stable := waitLeaderAmong(t, cluster.nodesByID(initial.leaderID, majorityFollower))

	require.Equal(t, initial.leaderID, stable.leaderID)
	require.Equal(t, initial.leaderTerm, stable.leaderTerm)
	require.Len(t, stable.followerIDs, 1)
	require.Equal(t, initial.leaderTerm, stable.terms[stable.followerIDs[0]])

	cluster.conditions.Heal(partitionedFollower, initial.leaderID)
	cluster.conditions.Heal(partitionedFollower, majorityFollower)

	cluster.waitHealthy()
}

func TestHighLatencyFollowerRecovers(t *testing.T) {
	t.Parallel()

	cluster := newHTTPNetworkTestCluster(t)
	cluster.Run(t.Context())

	initial := cluster.waitHealthy()
	slowFollower := initial.followerIDs[0]
	majorityFollower := initial.followerIDs[1]

	cluster.conditions.Latency(slowFollower, initial.leaderID, 80*time.Millisecond)
	cluster.conditions.Latency(slowFollower, majorityFollower, 80*time.Millisecond)

	waitLeaderAmong(t, cluster.nodesByID(initial.leaderID, majorityFollower))

	cluster.conditions.ClearLatency(slowFollower, initial.leaderID)
	cluster.conditions.ClearLatency(slowFollower, majorityFollower)

	recovered := cluster.waitHealthy()
	require.GreaterOrEqual(t, recovered.leaderTerm, initial.leaderTerm)
}

func TestClientCommandBlocksAndNodCommitedUntilReplicated(t *testing.T) {
	t.Parallel()

	cluster := newHTTPNetworkTestCluster(t)
	cluster.Run(t.Context())

	initial := cluster.waitHealthy()
	initialLeader := cluster.leader()
	cluster.conditions.Cut(initial.leaderID, initial.followerIDs[0])
	cluster.conditions.Cut(initial.leaderID, initial.followerIDs[1])

	t.Logf("send first command to %s", initialLeader.Id())
	cmd1 := []byte{'r', 'a', 'f', 't'}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	clientCommandResult := make(chan error)
	go func() {
		clientCommandResult <- initialLeader.ClientCommand(ctx, cmd1)
	}()
	time.Sleep(testingTimeouts.Election)
	cancel()
	require.ErrorIs(t, <-clientCommandResult, context.Canceled)
	require.Equal(t, 0, initialLeader.CommitIndex())
}

func TestClusterDiscardsCorruptedEntries(t *testing.T) {
	t.Parallel()

	cluster := newHTTPNetworkTestCluster(t)
	cluster.Run(t.Context())

	initial := cluster.waitHealthy()
	initialLeader := cluster.leader()
	cluster.conditions.Cut(initial.leaderID, initial.followerIDs[0])
	cluster.conditions.Cut(initial.leaderID, initial.followerIDs[1])

	t.Logf("send commands to %s", initialLeader.Id())
	initialLeaderCtx, cancel := context.WithCancel(t.Context())
	defer cancel()

	clientCommandResult := make(chan error, 2)
	go func() {
		clientCommandResult <- initialLeader.ClientCommand(initialLeaderCtx, []byte{'e', 'r', 'r'})
		clientCommandResult <- initialLeader.ClientCommand(initialLeaderCtx, []byte{'o', 'r'})
		close(clientCommandResult)
	}()

	time.Sleep(testingTimeouts.Election)
	cancel()

	for err := range clientCommandResult {
		require.ErrorIs(t, err, context.Canceled)
	}
	require.Equal(t, 0, initialLeader.CommitIndex())

	leader := cluster.nodesByID(waitLeaderAmong(t, cluster.nodesByID(initial.followerIDs...)).leaderID)[0]
	cmd3 := []byte{'h', 'e', 'a', 'l'}
	require.NoError(t, leader.ClientCommand(t.Context(), cmd3))

	cluster.conditions.Heal(initial.leaderID, initial.followerIDs[0])
	cluster.conditions.Heal(initial.leaderID, initial.followerIDs[1])
	cluster.waitNodesHaveCommitIndex(1, cluster.nodes)

	require.Equal(t, 1, initialLeader.StateMachine().Len())
	require.Equal(t, cmd3, initialLeader.StateMachine().Logs()[0].Data)
}

// TODO *Node instead of NodeId
type clusterSnapshot struct {
	leaderID    NodeId
	leaderTerm  int
	followerIDs []NodeId
	states      map[NodeId]State
	terms       map[NodeId]int
}

func waitLeaderAmong(t testing.TB, nodes []*Node) clusterSnapshot {
	maxRetries := 50
	retries := 0
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	stateMap := map[State][]NodeId{}
	for range ticker.C {
		retries++
		if retries == maxRetries {
			break
		}
		snapshot := snapshotNodes(nodes)
		stateMap = statesByState(snapshot.states)
		if snapshot.leaderID != None && len(snapshot.followerIDs) == len(nodes)-1 {
			return snapshot
		}
		time.Sleep(pollInterval)
	}
	t.Fatalf("no leader elected: %+v", stateMap)
	return clusterSnapshot{}
}

type networkTestCluster struct {
	t          testing.TB
	nodes      []*Node
	conditions *networkConditions
}

func newHTTPNetworkTestCluster(t testing.TB) *networkTestCluster {
	t.Helper()

	listeners := map[NodeId]net.Listener{}
	addrs := map[NodeId]string{}
	for _, id := range []NodeId{Node1, Node2, Node3} {
		ln, err := net.Listen("tcp", "0.0.0.0:0")
		require.NoError(t, err)

		listeners[id] = ln
		addrs[id] = ln.Addr().String()
	}

	conditions := newNetworkConditions(t)
	nodes := make([]*Node, 0, len(listeners))
	for _, id := range []NodeId{Node1, Node2, Node3} {
		nodeLogger := logger(t, id)
		base := NewHttpTransport(listeners[id], id, addrs, nodeLogger)
		node := NewNode(id, peersFor(id), nodeLogger, withNetworkConditions(id, base, conditions)).
			SetTimeouts(testingTimeouts)
		nodes = append(nodes, node)
	}

	cluster := &networkTestCluster{t: t, nodes: nodes, conditions: conditions}
	t.Cleanup(func() {
		cluster.Shutdown(t.Context())
	})
	return cluster
}

func (c *networkTestCluster) Run(ctx context.Context) {
	for _, n := range c.nodes {
		go n.Run(ctx)
	}
}

func (c *networkTestCluster) Shutdown(ctx context.Context) {
	for _, n := range c.nodes {
		n.Shutdown(ctx)
	}
}

func (c *networkTestCluster) leader() *Node {
	for _, n := range c.nodes {
		if n.State() == main.Leader {
			return n
		}
	}
	return nil
}

func (c *networkTestCluster) nodesByID(ids ...NodeId) []*Node {
	nodes := make([]*Node, 0, len(ids))
	for _, id := range ids {
		for _, n := range c.nodes {
			if n.Id() == id {
				nodes = append(nodes, n)
				break
			}
		}
	}
	return nodes
}

func (c *networkTestCluster) waitHealthy() clusterSnapshot {
	c.t.Helper()

	maxRetries := 50
	retries := 0
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	stateMap := map[State][]NodeId{}

	for range ticker.C {
		retries++
		if retries == maxRetries {
			break
		}
		snapshot := snapshotNodes(c.nodes)
		stateMap = statesByState(snapshot.states)
		if snapshot.leaderID != None && len(snapshot.followerIDs) == len(c.nodes)-1 && allTermsEqual(snapshot) {
			c.t.Logf("cluster is healthy: %+v", stateMap)
			return snapshot
		}
		time.Sleep(pollInterval)
	}
	c.t.Fatalf("cluster is not healthy: %+v", stateMap)
	return clusterSnapshot{}
}

func (c *networkTestCluster) waitNodesHaveCommitIndex(commitIndex int, nodes []*Node) {
	c.t.Helper()

	maxRetries := 50
	retries := 0
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	stateMap := map[State][]NodeId{}

	for range ticker.C {
		retries++
		if retries == maxRetries {
			break
		}
		assertion := true
		for _, n := range nodes {
			if n.CommitIndex() < commitIndex {
				assertion = false
				break
			}
		}
		if assertion {
			return
		}
		time.Sleep(pollInterval)
	}
	c.t.Fatalf("never commited index %d: %+v", commitIndex, stateMap)
}

func peersFor(id NodeId) []NodeId {
	switch id {
	case Node1:
		return []NodeId{Node2, Node3}
	case Node2:
		return []NodeId{Node1, Node3}
	case Node3:
		return []NodeId{Node1, Node2}
	default:
		panic(fmt.Sprintf("unknown node id %d", id))
	}
}

func snapshotNodes(nodes []*Node) clusterSnapshot {
	snapshot := clusterSnapshot{
		followerIDs: make([]NodeId, 0, len(nodes)-1),
		states:      make(map[NodeId]State, len(nodes)),
		terms:       make(map[NodeId]int, len(nodes)),
	}
	leaderCnt := 0
	for _, n := range nodes {
		id := n.Id()
		state := n.State()
		term := n.CurrentTerm()
		snapshot.states[id] = state
		snapshot.terms[id] = term
		switch state {
		case Leader:
			leaderCnt++
			snapshot.leaderID = id
			snapshot.leaderTerm = term
		case Follower:
			snapshot.followerIDs = append(snapshot.followerIDs, id)
		}
	}
	if leaderCnt != 1 {
		snapshot.leaderID = None
		snapshot.leaderTerm = 0
	}
	return snapshot
}

func allTermsEqual(snapshot clusterSnapshot) bool {
	for _, term := range snapshot.terms {
		if term != snapshot.leaderTerm {
			return false
		}
	}
	return true
}

func statesByState(states map[NodeId]State) map[State][]NodeId {
	stateMap := map[State][]NodeId{
		Leader:    make([]NodeId, 0, len(states)),
		Candidate: make([]NodeId, 0, len(states)),
		Follower:  make([]NodeId, 0, len(states)),
	}
	for id, state := range states {
		stateMap[state] = append(stateMap[state], id)
	}
	return stateMap
}

type coloredLogWriter struct {
	t      testing.TB
	color  string
	prefix string
}

var logColorMu sync.Mutex

func (w coloredLogWriter) Write(p []byte) (int, error) {
	logColorMu.Lock()
	defer logColorMu.Unlock()

	// TODO this causes data race sometimes
	if _, err := fmt.Fprintf(w.t.Output(), "%s%s\x1b[0m ", w.color, w.prefix); err != nil {
		return 0, err
	}
	return w.t.Output().Write(p)
}

func logger(t testing.TB, nodeID NodeId) *RaftLogger {
	level := slog.LevelDebug
	return NewRaftLogger(slog.New(slog.NewTextHandler(coloredLogWriter{
		t:      t,
		color:  nodeLogColors[nodeID],
		prefix: fmt.Sprintf("[Node%d]", nodeID),
	}, &slog.HandlerOptions{Level: level}))).DebugLevel(*flagDebugLevel)
}
