package storage

import (
	"sync"
)

type ListStorage struct {
	mu       sync.Mutex
	commands [][]byte
}

func (s *ListStorage) ApplyCommand(command []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.commands = append(s.commands, command)
}
