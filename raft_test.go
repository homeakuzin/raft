package main_test

import (
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	. "github.com/homeakuzin/raft"
	"github.com/stretchr/testify/assert"
)

var flagDebugLevel = flag.Int("debug", 0, "node debug level")

var nodeLogColors = map[NodeId]string{
	Node1: "\x1b[34m",
	Node2: "\x1b[32m",
	Node3: "\x1b[35m",
}

func TestMain(m *testing.M) {
	flag.Parse()
	os.Exit(m.Run())
}

func TestLeaderIsElected(t *testing.T) {
	t.Parallel()

	ln1, err := net.Listen("tcp", "0.0.0.0:0")
	assert.NoError(t, err)
	ln2, err := net.Listen("tcp", "0.0.0.0:0")
	assert.NoError(t, err)
	ln3, err := net.Listen("tcp", "0.0.0.0:0")
	assert.NoError(t, err)

	addrs := map[NodeId]string{
		Node1: ln1.Addr().String(),
		Node2: ln2.Addr().String(),
		Node3: ln3.Addr().String(),
	}
	logger1 := logger(t, Node1)
	logger2 := logger(t, Node2)
	logger3 := logger(t, Node3)
	n1 := NewNode(Node1, []NodeId{Node2, Node3}, logger1, NewHttpTransport(ln1, Node1, addrs, logger1))
	n2 := NewNode(Node2, []NodeId{Node1, Node3}, logger2, NewHttpTransport(ln2, Node2, addrs, logger2))
	n3 := NewNode(Node3, []NodeId{Node1, Node2}, logger3, NewHttpTransport(ln3, Node3, addrs, logger3))
	ctx := t.Context()
	nodes := []*Node{n1, n2, n3}
	defer func() {
		for _, n := range nodes {
			n.Shutdown(ctx)
		}
	}()
	go n1.Run(ctx)
	go n2.Run(ctx)
	go n3.Run(ctx)
	leader, followers := waitForLeader(t, nodes)
	for _, n := range followers {
		assert.Equal(t, leader.CurrentTerm(), n.CurrentTerm())
	}
}

func waitForLeader(t testing.TB, nodes []*Node) (leader *Node, followers []*Node) {
	maxRetries := 50
	retries := 0
	pollInterval := time.Millisecond * 15
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for range ticker.C {
		retries++
		if retries == maxRetries {
			break
		}
		t.Log("poll states")
		stateMap := map[State][]NodeId{
			Leader:    make([]NodeId, 0, len(nodes)),
			Candidate: make([]NodeId, 0, len(nodes)),
			Follower:  make([]NodeId, 0, len(nodes)),
		}
		followers = make([]*Node, 0, len(nodes))
		leaderCnt := 0
		for _, n := range nodes {
			state := n.State()
			if state == Leader {
				leaderCnt++
				leader = n
			} else if state == Follower {
				followers = append(followers, n)
			}
			stateMap[state] = append(stateMap[state], n.Id())
		}
		if leaderCnt == 1 && len(followers) == 2 {
			return
		}
		assert.LessOrEqual(t, leaderCnt, 1)
		time.Sleep(pollInterval)
	}
	t.Fatal("no leader elected")
	return
}

type coloredLogWriter struct {
	w      io.Writer
	color  string
	prefix string
}

var logColorMu sync.Mutex

func (w coloredLogWriter) Write(p []byte) (int, error) {
	logColorMu.Lock()
	defer logColorMu.Unlock()

	if _, err := fmt.Fprintf(w.w, "%s%s\x1b[0m ", w.color, w.prefix); err != nil {
		return 0, err
	}
	return w.w.Write(p)
}

func logger(t testing.TB, nodeID NodeId) *RaftLogger {
	level := slog.LevelDebug
	return NewRaftLogger(slog.New(slog.NewTextHandler(coloredLogWriter{
		w:      t.Output(),
		color:  nodeLogColors[nodeID],
		prefix: fmt.Sprintf("[Node%d]", nodeID),
	}, &slog.HandlerOptions{Level: level}))).DebugLevel(*flagDebugLevel)
}
