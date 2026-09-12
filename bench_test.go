package main_test

import (
	"log/slog"
	"testing"

	main "github.com/homeakuzin/raft"
	"github.com/stretchr/testify/require"
)

func BenchmarkCommandsToHealthyCluster(b *testing.B) {
	cluster := newTestClusterWithLoggerFactory(b, func(t testing.TB, id main.NodeId) *main.RaftLogger {
		return main.NewRaftLogger(slog.New(slog.DiscardHandler))
	})
	cluster.Run(b.Context())
	cluster.waitHealthy()
	leader := cluster.leader()
	for b.Loop() {
		require.NoError(b, leader.ClientCommand(b.Context(), []byte{'l', 'o', 'g'}))
	}
}
