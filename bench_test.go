package main_test

import (
	"log/slog"
	"math/rand"
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

	n := 1024
	commands := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		commands[i] = generateCommand(n)
		require.NoError(b, leader.ClientCommand(b.Context(), commands[i]))
	}

	logs := leader.StateMachine().Logs()
	for i := range commands {
		require.Equal(b, commands[i], logs[i].Data)
	}
}

func generateCommand(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		for {
			var one [1]byte
			if _, err := rand.Read(one[:]); err != nil {
				panic(err)
			}
			if one[0] < 190 {
				b[i] = 32 + one[0]%95
				break
			}
		}
	}

	return b
}
